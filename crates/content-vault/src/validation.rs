use image::{ImageFormat, ImageReader, Limits};
use std::io::Cursor;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationErrorKind {
    UnsupportedMediaType,
    MediaTypeMismatch,
    DecodeFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{kind:?}: {message}")]
pub struct ValidationError {
    kind: ValidationErrorKind,
    message: String,
}

impl ValidationError {
    pub fn new(kind: ValidationErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> ValidationErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

pub trait ContentValidator: std::fmt::Debug + Send + Sync {
    fn supports(&self, media_type: &str) -> bool;
    fn validate(&self, media_type: &str, bytes: &[u8]) -> Result<(), ValidationError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BasicContentValidator;

impl ContentValidator for BasicContentValidator {
    fn supports(&self, media_type: &str) -> bool {
        matches!(media_type, "image/png" | "image/jpeg" | "text/plain")
    }

    fn validate(&self, media_type: &str, bytes: &[u8]) -> Result<(), ValidationError> {
        match media_type {
            "image/png" => decode_image(bytes, ImageFormat::Png),
            "image/jpeg" => decode_image(bytes, ImageFormat::Jpeg),
            "text/plain" => {
                let text = std::str::from_utf8(bytes).map_err(|_| {
                    ValidationError::new(
                        ValidationErrorKind::MediaTypeMismatch,
                        "text/plain content is not valid UTF-8",
                    )
                })?;
                if text.contains('\0') {
                    return Err(ValidationError::new(
                        ValidationErrorKind::MediaTypeMismatch,
                        "text/plain content contains a NUL byte",
                    ));
                }
                Ok(())
            }
            _ => Err(ValidationError::new(
                ValidationErrorKind::UnsupportedMediaType,
                "media type is not supported by the configured validator",
            )),
        }
    }
}

fn decode_image(bytes: &[u8], expected: ImageFormat) -> Result<(), ValidationError> {
    let detected = image::guess_format(bytes).map_err(|_| {
        ValidationError::new(
            ValidationErrorKind::MediaTypeMismatch,
            "image signature does not match the declared media type",
        )
    })?;
    if detected != expected {
        return Err(ValidationError::new(
            ValidationErrorKind::MediaTypeMismatch,
            "image signature does not match the declared media type",
        ));
    }

    let mut reader = ImageReader::with_format(Cursor::new(bytes), expected);
    let mut limits = Limits::default();
    limits.max_image_width = Some(32_768);
    limits.max_image_height = Some(32_768);
    limits.max_alloc = Some(256 * 1024 * 1024);
    reader.limits(limits);
    reader.decode().map(|_| ()).map_err(|_| {
        ValidationError::new(
            ValidationErrorKind::DecodeFailed,
            "image decoder rejected the content",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_validation_rejects_binary_content() {
        let error = BasicContentValidator
            .validate("text/plain", &[0xff, 0x00])
            .unwrap_err();
        assert_eq!(error.kind(), ValidationErrorKind::MediaTypeMismatch);
    }

    #[test]
    fn media_types_are_explicit() {
        assert!(BasicContentValidator.supports("image/png"));
        assert!(!BasicContentValidator.supports("application/octet-stream"));
    }
}
