from faststream.exceptions import FastStreamException


class DatetimeMissingTimezoneException(FastStreamException):
    def __str__(self) -> str:
        return "This requires a datetime with a non-None timezone"