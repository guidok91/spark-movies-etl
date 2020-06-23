from datautils.logging import logger


class SlackClient(object):
    def __init__(self, token, channel, username):
        self.token = token
        self.channel = channel
        self.username = username

    def report_to_slack(self, message: str):
        # TODO: implement in the future!
        logger.error(f"ERROR (reported to Slack - to be implemented).\nMESSAGE: {message}")
        pass
