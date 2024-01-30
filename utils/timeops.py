from datetime import datetime, timedelta
import pytz


class TimeHelper:
    def __init__(self):
        pass

    def datetime_to_string(self, dt: datetime, format_str: str = '%Y-%m-%d %H:%M:%S') -> str:
        """
        Convert a datetime object to a string.

        Args:
            dt (datetime): The datetime object to be converted.
            format_str (str, optional): The format string for the output. Defaults to '%Y-%m-%d %H:%M:%S'.

        Returns:
            str: Datetime object converted to a string.
        """
        return dt.strftime(format_str)

    def string_to_datetime(self, date_str: str, format_str: str = '%Y-%m-%d %H:%M:%S') -> datetime:
        """
        Convert a string to a datetime object.

        Args:
            date_str (str): The string representing a datetime.
            format_str (str, optional): The format string of the input. Defaults to '%Y-%m-%d %H:%M:%S'.

        Returns:
            datetime: String converted to a datetime object.
        """
        return datetime.strptime(date_str, format_str)

    def get_current_datetime(self) -> datetime:
        """
        Get the current datetime.

        Returns:
            datetime: Current datetime.
        """
        return datetime.now()

    def get_current_timestamp(self) -> int:
        """
        Get the current datetime in timestamp format.

        Returns:
            int: Current datetime in timestamp format.
        """
        return int(datetime.now().timestamp())

    def get_current_datetime_string(self, format_str: str = '%Y-%m-%d %H:%M:%S') -> str:
        """
        Get the current datetime as a string without timezone information.

        Args:
            format_str (str): String datetime format
        Returns:
            str: Current datetime as a string without timezone.
        """
        return datetime.now().strftime(format_str)

    def __generate_new_datetime_with_time_delta(self, seconds: int, minutes: int, hours: int, days: int,
                                                dt_value: datetime, operation: str = 'minus') -> datetime:
        """
        Generate a new datetime object by adding or subtracting a specified duration.

        Args:
            seconds (int): Number of seconds to add or subtract. Defaults to 0.
            minutes (int): Number of minutes to add or subtract. Defaults to 0.
            hours (int): Number of hours to add or subtract. Defaults to 0.
            days (int): Number of days to add or subtract. Defaults to 0.
            dt_value (datetime): The reference datetime. If None, use the current datetime.
            operation (str): The operation to perform ('minus' or 'plus'). Defaults to 'minus'.

        Returns:
            datetime: New datetime object after adding or subtracting the specified duration.
        """
        if dt_value is None:
            dt_value = datetime.now()

        delta = timedelta(seconds=seconds, minutes=minutes, hours=hours, days=days)

        if operation == 'minus':
            return dt_value - delta
        elif operation == 'plus':
            return dt_value + delta

    def generate_before_datetime_timedelta(self, seconds: int = 0, minutes: int = 0, hours: int = 0,
                                           days: int = 0, dt_value: datetime = None) -> datetime:
        """
        Generate a datetime object representing a specified duration before the given datetime.

        Args:
            seconds (int, optional): Number of seconds before the current datetime. Defaults to 0.
            minutes (int, optional): Number of minutes before the current datetime. Defaults to 0.
            hours (int, optional): Number of hours before the current datetime. Defaults to 0.
            days (int, optional): Number of days before the current datetime. Defaults to 0.
            dt_value (datetime, optional): The reference datetime. If None, use the current datetime.

        Returns:
            datetime: Datetime object representing the specified duration before the reference datetime.
        """
        return self.__generate_new_datetime_with_time_delta(seconds, minutes, hours, days, dt_value, operation='minus')

    def generate_next_datetime_timedelta(self, seconds: int = 0, minutes: int = 0, hours: int = 0, days: int = 0,
                                         dt_value: datetime = None, ) -> datetime:
        """
        Generate a datetime object representing a specified duration after the given datetime.

        Args:
            seconds (int, optional): Number of seconds after the current datetime. Defaults to 0.
            minutes (int, optional): Number of minutes after the current datetime. Defaults to 0.
            hours (int, optional): Number of hours after the current datetime. Defaults to 0.
            days (int, optional): Number of days after the current datetime. Defaults to 0.
            dt_value (datetime, optional): The reference datetime. If None, use the current datetime.

        Returns:
            datetime: Datetime object representing the specified duration after the reference datetime.
        """
        return self.__generate_new_datetime_with_time_delta(seconds, minutes, hours, days, dt_value, operation='plus')

    def generate_istanbul_datetime(self, year: int, month: int, day: int,
                                   hour: int, minute: int, second: int) -> datetime:
        """
        Generate a datetime object with the Istanbul timezone.

        Args:
            year (int): The year.
            month (int): The month.
            day (int): The day.
            hour (int): The hour.
            minute (int): The minute.
            second (int): The second.

        Returns:
            datetime: Datetime object with the Istanbul timezone.
        """
        timezone_name = 'Europe/Istanbul'
        istanbul_timezone = pytz.timezone(timezone_name)

        dt_without_tz = datetime(year, month, day, hour, minute, second)
        dt_with_istanbul_tz = istanbul_timezone.localize(dt_without_tz)

        return dt_with_istanbul_tz

    def convert_string_to_datetime_with_timezone(self, datetime_str: str, timezone_str: str = "Europe/Istanbul",
                                                 format_str='%Y-%m-%d %H:%M:%S') -> datetime:
        """
        Convert a string representation of a datetime with a specified timezone to a datetime object.

        Args:
            datetime_str (str): The string representation of a datetime.
            timezone_str (str): The timezone string, e.g., 'America/New_York'.
            format_str (str, optional): The format string of the input. Defaults to '%Y-%m-%d %H:%M:%S'.

        Returns:
            datetime: Datetime object with the specified timezone.
        """
        timezone = pytz.timezone(timezone_str)
        dt_with_tz = datetime.strptime(datetime_str, format_str)
        dt_with_tz = timezone.localize(dt_with_tz)
        return dt_with_tz
