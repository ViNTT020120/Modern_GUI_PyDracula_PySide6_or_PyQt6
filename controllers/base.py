import asyncio
from PyQt5.QtCore import QObject, pyqtSignal, QThread, pyqtSlot
from PyQt5.QtWidgets import QMainWindow, QApplication
from models.database_manager.base import BaseDatabaseManager
from models.socket_manager.kis import KISSocketManager
from models.socket_manager.ssi import SSISocketManager
import logging

import sys


class BaseController(QObject):
    dashboard_etf_signal = pyqtSignal(dict)
    dashboard_futures_signal = pyqtSignal(dict)

    def __init__(self):
        super().__init__(parent=None)
        self.loop = asyncio.new_event_loop()
        self.database_manager = BaseDatabaseManager()
        self.socket_manager = SSISocketManager(self.database_manager, self.dashboard_etf_signal,
                                               self.dashboard_futures_signal)
        logging.basicConfig(level=logging.DEBUG)

    def run(self):
        asyncio.set_event_loop(self.loop)
        logging.debug("Event loop set")
        # First run refresh_database, and once it's done, run connect
        future_db = asyncio.run_coroutine_threadsafe(self.refresh_database(), self.loop)
        logging.debug("Scheduled refresh_database")
        # Schedule connect() to run after refresh_database completes
        future_db.add_done_callback(lambda _: asyncio.run_coroutine_threadsafe(self.connect(), self.loop))
        logging.debug("Scheduled connect after refresh_database")
        # Start the event loop
        self.loop.run_forever()
        logging.debug("Event loop started")

    async def refresh_database(self):
        # Refresh database (returns None)
        logging.debug("Refreshing database")
        await self.database_manager.refresh_database()
        logging.debug("Database refreshed")

    async def connect(self):
        # Run the websocket connection (continues running)
        logging.debug("Connecting to WebSocket")
        await self.socket_manager.connect()
        logging.debug("Connected to WebSocket")

    def schedule_task(self, task):
        """Schedules an async task to run in the event loop."""
        asyncio.run_coroutine_threadsafe(task, self.loop)
        logging.debug(f"Scheduled task: {task}")


class Window(QMainWindow):
    def __init__(self):
        super().__init__(parent=None)
        self.setWindowTitle("Real-Time Stock Price and Volume Bar Chart")
        self.setGeometry(100, 100, 800, 600)
        self.runLongTask()

    def runLongTask(self):
        self.thread = QThread()
        self.worker = BaseController()
        self.worker.moveToThread(self.thread)
        self.worker.moveToThread(self.thread)
        self.thread.started.connect(self.worker.run)
        self.thread.finished.connect(self.thread.deleteLater)
        self.thread.start()

        self.worker.dashboard_etf_signal.connect(self.update_dashboard_etf)
        self.worker.dashboard_futures_signal.connect(self.update_dashboard_futures)

    @pyqtSlot(dict)
    def update_dashboard_etf(self, etf_data):
        print(etf_data)

    @pyqtSlot(dict)
    def update_dashboard_futures(self, futures_data):
        print(futures_data)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = Window()
    win.show()
    sys.exit(app.exec())
