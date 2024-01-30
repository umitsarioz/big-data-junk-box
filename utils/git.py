import logging
import subprocess
import traceback
from typing import List


class GitHelper:
    def __init__(self, logger: logging.Logger, user_name_surname: str, user_mail: str, scenario_no: str):
        """
        logger (logging.Logger): logger object for logging message
        user_name_surname (str): Developer username and surname. 
        user_mail (str): Developer e-mail
        scenario_no: Scenario no. It means branch name of the scenario in bucket. Ex. OYZ-3822
        """

        self._logger = logger
        self._user_name_surname = user_name_surname
        self._user_mail = user_mail
        self._scenario_no = scenario_no

    def __run_command(self, cmd: str) -> str:
        stdout = subprocess.getoutput(cmd)
        return stdout

    def add_commit(self, commit_message: str, debug=False):
        self._logger.info("Adding commit as " + commit_message)
        cmd = f'git commit --author "{self._user_name_surname} <{self._user_mail}>" --message "{commit_message}"'
        try:
            stdout = self.__run_command(cmd)
            if debug: self._logger.info(stdout)
        except UnicodeEncodeError:
            self._logger.error("Türkçe karakter hatası.İsim soyisim ya da mail bilginizi kontrol ediniz.")

    def add_files(self, files: List[str] = None, debug=False):
        if files is None:
            files = list()

        files = ' '.join(files) if files else '.'
        self._logger.info("Adding files: " + 'All(.) ' if files == '.' else files)
        cmd = f"git add -A {files}"

        stdout = self.__run_command(cmd)
        if debug: self._logger.info(stdout)

    def push(self, debug=False):
        self._logger.info("Pushing files to bitbucket.")
        cmd = f"git push origin {self._scenario_no}"
        stdout = self.__run_command(cmd)
        if debug: self._logger.info(stdout)

    def exec_push_pipeline(self, commit_message: str, files: List[str] = None, debug=False):
        if files is None:
            files = list()
        try:
            self.add_files(files, debug)
            self.add_commit(commit_message, debug)
            self.push(debug)
            self._logger.info("OK")
        except:
            self._logger.error("Push ederken hata olustu.Hata detay:", traceback.format_exc())

    def find_file_in_git_commit_history(self, target_file_name: str):
        res = None

        commit_ids = [row.split()[1] for row in subprocess.getoutput("git log --reflog").split('\n')
                      if 'commit' in row and len(row) > 45]

        for idx, commit_id in enumerate(commit_ids):
            if res: break
            self._logger.info(f"{idx + 1}/{len(commit_ids)} > Commit ID:", commit_id)
            files = subprocess.getoutput(f"git diff-tree --no-commit-id --name-only {commit_id} -r").split('\n')
            for file in files:
                if res: break
                if target_file_name in file:
                    res = "Dosya bulundu. Commit ID:" + commit_id

        if not res:
            res = "Dosya hiçbir commit'de bulunamadı"

        self._logger.info(res)
