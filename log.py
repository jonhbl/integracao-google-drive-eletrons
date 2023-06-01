import logging
from gdrive import *
import os

base_dir = os.getcwd()
log_dir = os.path.join(base_dir, "logs")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

formatter = logging.Formatter(
    fmt="%(asctime)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S"
)

logger = logging.getLogger("gdrive")
logger.setLevel("DEBUG")

fileHandler = logging.FileHandler(os.path.join(log_dir, "log.txt"), "w")
fileHandlerDebug = logging.FileHandler(os.path.join(log_dir, "debug.txt"), "w")
streamHandler = logging.StreamHandler()

streamHandler.setLevel("WARNING")
fileHandler.setLevel("INFO")

streamHandler.setFormatter(formatter)
fileHandler.setFormatter(formatter)
fileHandlerDebug.setFormatter(formatter)

logger.addHandler(fileHandler)
logger.addHandler(fileHandlerDebug)
logger.addHandler(streamHandler)
