import logging
from datetime import datetime
from smtplib import SMTP
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from os.path import basename
from email.utils import COMMASPACE, formatdate
from typing import List


class MailHelper:
    HTML_BEGINNING = "<html><head></head><body><p>Selamlar,<br><br>"
    HTML_ENDING = "<br><br>İyi çalışmalar,</p></body></html>"

    def __init__(self, logger: logging.Logger):
        """
        Initialize the MailHelper instance.
        """
        self.logger = logger
        self.today_str = datetime.now().strftime("%Y-%m-%d")
        self._mail_from = None
        self._mail_server = None
        self._mail_to = []
        self._mail_cc = []
        self._mail_bcc = []

    def _create_mail_blueprint(self, subject: str, mail_to: List[str], mail_cc: List[str],
                               mail_bcc: List[str]) -> MIMEMultipart:
        """
        Create the basic email structure.

        Parameters:
            subject (str): Email subject.
            mail_to (List[str]): List of recipients.
            mail_cc (List[str]): List of CC recipients.
            mail_bcc (List[str]): List of BCC recipients.

        Returns:
            MIMEMultipart: Email message object.
        """
        msg = MIMEMultipart()
        msg['From'] = self._mail_from
        msg['To'] = COMMASPACE.join(mail_to)
        msg['Cc'] = COMMASPACE.join(mail_cc)
        msg['Bcc'] = COMMASPACE.join(mail_bcc)
        msg['Date'] = formatdate(localtime=False)
        msg['Subject'] = f"{subject} - {self.today_str} "
        return msg

    def _set_mail_content(self, msg_obj: MIMEMultipart, msg_text: str, files: List[str] = None) -> MIMEMultipart:
        """
        Set the content of the email.

        Parameters:
            msg_obj (MIMEMultipart): Email message object.
            msg_text (str): Email body text.
            files (List[str]): List of file paths to be attached.

        Returns:
            MIMEMultipart: Updated email message object.
        """
        part1 = MIMEText(msg_text, 'html')
        msg_obj.attach(part1)
        for curr_file in files or []:
            try:
                with open(curr_file, "rb") as cf:
                    part = MIMEApplication(cf.read(), Name=basename(curr_file))
                part['Content-Disposition'] = f'attachment; filename="{basename(curr_file)}"'
                msg_obj.attach(part)
            except Exception as e:
                err_msg = f"Error attaching file {curr_file}: {e}"
                self.logger.warning(err_msg)
                raise Exception(err_msg)
        return msg_obj

    def _send_mail(self, msg_obj: MIMEMultipart, mail_from: str, mail_to: List[str], mail_cc: List[str],
                   mail_bcc: List[str]):
        """
        Send the email.

        Parameters:
            msg_obj (MIMEMultipart): Email message object.
            mail_from (str): Sender's email address.
            mail_to (List[str]): List of recipients.
            mail_cc (List[str]): List of CC recipients.
            mail_bcc (List[str]): List of BCC recipients.
        """
        self.logger.info("Sending email...")
        try:
            with SMTP(self._mail_server) as smtp:
                from_addr, to_addrs = mail_from, mail_to + mail_cc + mail_bcc
                smtp.sendmail(from_addr=from_addr, to_addrs=to_addrs, msg=msg_obj.as_string())
            self.logger.info(f"Email sent successfully. Recipient email addresses: {to_addrs}")
        except Exception as e:
            err_msg = f"Error sending email: {e}"
            self.logger.error(err_msg)
            raise Exception(err_msg)


    def set_sending_parameters(self, mail_server: str, mail_from: str):
        """
        Set the sender's email address and mail server.

        Parameters:
            mail_from (str): Sender's email address.
            mail_server (str): Sender's email server.
        """
        if not mail_server:
            raise ValueError("Mail server address cannot be empty.")
        if not mail_from:
            raise ValueError("Mail from address cannot be empty.")

        self._mail_server = mail_server
        self._mail_from = mail_from

    def get_sending_parameters(self) -> dict:
        """Get the sender's email address and mail server."""
        return {'mail_server:': self._mail_server, 'mail_from': self._mail_from}

    def send_scenario_mail(self, mail_to: List[str], mail_cc: List[str], mail_bcc: List[str], mail_subject: str,
                           mail_text: str, files: List[str] = None):
        """
        Send a scenario-based email.

        Parameters:
            mail_to (List[str]): List of recipients.
            mail_cc (List[str]): List of CC recipients.
            mail_bcc (List[str]): List of BCC recipients.
            mail_subject (str): Email subject.
            mail_text (str): Email body text.
            files (List[str]): List of file paths to be attached.
        """
        # Check if important parameters are defined
        if not all([self._mail_from, self._mail_server]):
            self.logger.error("Mail from address and mail server address must be defined.")
            raise Exception("Mail from address and mail server address must be defined.")

        msg = self._create_mail_blueprint(subject=mail_subject, mail_to=mail_to, mail_cc=mail_cc, mail_bcc=mail_bcc)
        mail_text = self.HTML_BEGINNING + mail_text + self.HTML_ENDING
        msg = self._set_mail_content(msg_obj=msg, msg_text=mail_text, files=files)
        self._send_mail(msg_obj=msg, mail_from=self._mail_from, mail_to=mail_to, mail_cc=mail_cc,
                        mail_bcc=mail_bcc)
