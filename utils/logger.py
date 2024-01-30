import logging

class LogHelper:
    def __init__(self,log_level='info',filename=''):
        """
        Args:
            log_level (str): Logging log level definition. Default value is info. Alternatives are warn or error.
            filename (str): Logging filename or filepath.

        """
        self._log_level = log_level
        self._filename = filename
        self._logger = None
    
    def _set_log_level_value(self):
        if self._log_level == 'info':
            log_level_value = logging.INFO
        elif self._log_level == 'warn':
            log_level_value = logging.WARNING
        elif self._log_level == 'error':
            log_level_value = logging.ERROR
        else:
            raise Exception("Undefined log level. Log level can be info,warn or error.")
    
        return log_level_value
    
    
    def create_logger(self) -> logging.Logger:
        """
        Create logger object and return this object.
     
        Returns:
            logger (logging.Logger): Logger object
        """
        log_level_value = self._set_log_level_value()

        if not self._filename:
            logging.basicConfig(level=log_level_value,
                            format='%(asctime)s | [%(levelname)s] :: %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S'
                            )
        else:
            self._filename = self._filename if self._filename.endswith('.log') else f"{self._filename}.log"
            logging.basicConfig(level=log_level_value,
                    format='%(asctime)s | [%(levelname)s] :: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename=self._filename,
                    encoding='utf-8'
                    )

        self._logger = logging.getLogger(__name__)
        return self._logger
    
    def change_log_level(self,new_level:str):
        self._log_level = new_level
        log_level_value = self._set_log_level_value()
        self._logger.setLevel(log_level_value)
   
    def get_logger_object(self) -> logging.Logger:
        return self._logger
    
    def get_logger_level(self) -> str:
        return self._log_level
    