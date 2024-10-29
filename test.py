import sys
from controllers.base import BaseController
from PyQt5 import QtCore, QtWidgets, QtGui
from PyQt5.QtChart import QChart, QChartView, QBarSet, QBarSeries, QBarCategoryAxis, QValueAxis
from PyQt5.QtGui import QPainter
from PyQt5.QtCore import QPoint, Qt
from mapping import *


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.menu_widget = MenuWidget(parent=self)
        self.setMenuBar(self.menu_widget)
        self.table_widget = TableWidget(parent=self)
        self.setCentralWidget(self.table_widget)


class MenuWidget(QtWidgets.QMenuBar):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.createMenuBar()

    def createMenuBar(self):
        # File menu
        file_menu = self.addMenu('File')
        # adding actions to file menu
        open_action = QtWidgets.QAction('Open', self)
        close_action = QtWidgets.QAction('Close', self)
        file_menu.addAction(open_action)
        file_menu.addAction(close_action)
        # Edit menu
        edit_menu = self.addMenu('&Edit')
        # adding actions to edit menu
        undo_action = QtWidgets.QAction('Undo', self)
        redo_action = QtWidgets.QAction('Redo', self)
        edit_menu.addAction(undo_action)
        edit_menu.addAction(redo_action)

        # use `connect` method to bind signals to desired behavior
        close_action.triggered.connect(self.close)

        helpMenu = self.addMenu("&Help")


class TableWidget(QtWidgets.QTabWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.createFutureTable()
        self.createEtfTable()
        self.createIndexTable()
        self.drawChart()

        layout = QtWidgets.QVBoxLayout(self)
        layout1 = QtWidgets.QHBoxLayout(self)

        layout1.addWidget(self.future_table, stretch=7)
        layout1.addWidget(self.index_table, stretch=2)
        layout1.addWidget(QtWidgets.QFrame(self), stretch=2)
        layout.addLayout(layout1)

        layout.addWidget(self.etf_table, stretch=2)
        layout.addWidget(self.chart_view, stretch=2)

        self.setLayout(layout)
        self.runLongTask()

    def createEtfTable(self):
        self.rows_etf = ["E1VFVN30", "FUEVFVND", "FUESSVFL", "FUESSV30",
                         "FUESSV50", "FUEVN100", "FUEKIV30", "FUEDCMID",
                         "FUEKIVFS", "FUEMAVND", "FUEKIVND", "FUEMAV30"]
        self.cols_etf = ["ETF", "iNAV basket",
                         "Best bid", "ETF bid premium", "ETF bid volume",
                         "Best ask", "ETF ask premium", "ETF ask volume",
                         "Basket bid premium", "Basket ask premium",
                         "Realtime hedge ratio", "NFOL hedge ratio",
                         "FOL hedge ratio", "Per 1 F contract",
                         "Risk premium"]

        self.etf_table = QtWidgets.QTableWidget()
        self.etf_table.setFont(QtGui.QFont('Arial', 11))
        self.etf_table.setColumnCount(14)
        self.etf_table.setRowCount(12)

        self.etf_table.setHorizontalHeaderLabels(self.cols_etf)

        for i in range(self.etf_table.rowCount()):
            self.etf_table.setItem(i, 0, QtWidgets.QTableWidgetItem(self.rows_etf[i]))
            font = QtGui.QFont()
            font.setBold(True)
            self.etf_table.item(i, 0).setFont(font)
            for j in range(0, self.etf_table.columnCount() - 1):
                self.etf_table.setItem(i, j + 1, QtWidgets.QTableWidgetItem("-0.0045%"))
                if self.cols_etf[j + 1] in ["iNAV basket", "Best ask", "ETF ask premium", "ETF ask volume",
                                            "Basket ask premium"]:
                    it = self.etf_table.item(i, j + 1)
                    it.setForeground(QtGui.QBrush(QtGui.QColor("red")))
                elif self.cols_etf[j + 1] in ["Best bid", "ETF bid premium", "ETF bid volume", "Basket bid premium"]:
                    it = self.etf_table.item(i, j + 1)
                    it.setForeground(QtGui.QBrush(QtGui.QColor("#286fa6")))
                elif self.cols_etf[j + 1] == "Realtime hedge ratio":
                    it = self.etf_table.item(i, j + 1)
                    it.setFont(font)
                    it.setBackground(QtGui.QBrush(QtGui.QColor("#a5cfe8")))
                    it.setForeground(QtGui.QBrush(QtGui.QColor("#286fa6")))

        self.etf_table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)
        self.etf_table.setSizePolicy(QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Policy.Minimum)

    def createFutureTable(self):
        self.cols_future = ["Future", "Time to M", "Bid price", "Bid Vol",
                            "Ask price", "Ask Vol", "Basis", "Effective rate",
                            "Roll"]

        self.rows_future = ["VNC1", "VNC2"]

        self.future_table = QtWidgets.QTableWidget()
        self.future_table.setFont(QtGui.QFont('Arial', 11))
        self.future_table.setColumnCount(9)
        self.future_table.setRowCount(2)

        self.future_table.setHorizontalHeaderLabels(self.cols_future)

        for i in range(self.future_table.rowCount()):
            self.future_table.setItem(i, 0, QtWidgets.QTableWidgetItem(self.rows_future[i]))
            font = QtGui.QFont()
            font.setBold(True)
            self.future_table.item(i, 0).setFont(font)
            for j in range(1, self.future_table.columnCount()):
                self.future_table.setItem(i, j + 1, QtWidgets.QTableWidgetItem("0.999%"))

        self.future_table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)
        self.future_table.setSizePolicy(QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Policy.Minimum)

    def createIndexTable(self):
        self.cols_index = ["VN30 Index", "VNFL Index"]

        self.index_table = QtWidgets.QTableWidget()
        self.index_table.setFont(QtGui.QFont('Arial', 11))

        self.index_table.setColumnCount(2)
        self.index_table.setRowCount(1)

        self.index_table.setHorizontalHeaderLabels(self.cols_index)

        for j in range(self.index_table.columnCount()):
            self.index_table.setItem(0, j, QtWidgets.QTableWidgetItem(self.cols_index[j]))
            for i in range(self.index_table.rowCount()):
                self.index_table.setItem(i, j, QtWidgets.QTableWidgetItem("0"))

        self.index_table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)
        self.index_table.setSizePolicy(QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Policy.Minimum)

    def runLongTask(self):
        self.thread = QtCore.QThread()
        self.worker = BaseController()
        self.worker.moveToThread(self.thread)
        self.thread.started.connect(self.worker.run)
        self.worker.dashboard_etf_signal.connect(self.update_etf)
        self.worker.dashboard_futures_signal.connect(self.update_future)
        self.thread.start()

    @QtCore.pyqtSlot(dict)
    def update_etf(self, dict_data):
        print(dict_data)
        etf = dict_data['ticker']
        r = RowTable[etf].value
        del dict_data['ticker']
        for col in dict_data:
            c = ColTable[col].value
            item = dict_data[col]
            it = QtWidgets.QTableWidgetItem(item)
            # it.setForeground(QBrush(QColor("red")))
            self.etf_table.setItem(r, c, it)

    @QtCore.pyqtSlot(dict)
    def update_future(self, dict_data):
        print(dict_data)
        future = dict_data['ticker']
        if "roll_1" in dict_data.keys():
            item1 = dict_data["roll_1"]
            item2 = dict_data["roll_2"]
            if future == 'VNC1':
                r1 = RowTable.VNC1.value
                r2 = RowTable.VNC2.value
            else:
                r1 = RowTable.VNC2.value
                r2 = RowTable.VNC1.value
            c = ColTable.roll.value
            it1 = QtWidgets.QTableWidgetItem(item1)
            it2 = QtWidgets.QTableWidgetItem(item2)
            self.future_table.setItem(r1, c, it1)
            self.future_table.setItem(r2, c, it2)
            del dict_data['roll_1']
            del dict_data['roll_2']

        r = RowTable[future].value
        del dict_data['ticker']
        for col in dict_data:
            c = ColTable[col].value
            item = dict_data[col]
            it = QtWidgets.QTableWidgetItem(item)
            self.future_table.setItem(r, c, it)

    def drawChart(self):
        set0 = QBarSet("Jane")
        set1 = QBarSet("John")
        set2 = QBarSet("Axel")
        set3 = QBarSet("Mary")
        set4 = QBarSet("Sam")

        set0.append([1, 2, 3, 4, 5, 6])
        set1.append([5, 0, 0, 4, 0, 7])
        set2.append([3, 5, 8, 13, 8, 5])
        set3.append([5, 6, 7, 3, 4, 5])
        set4.append([9, 7, 5, 3, 1, 2])

        bar_series = QBarSeries()
        bar_series.append(set0)
        bar_series.append(set1)
        bar_series.append(set2)
        bar_series.append(set3)
        bar_series.append(set4)

        chart = QChart()
        chart.addSeries(bar_series)
        chart.setTitle("Bar chart example")

        categories = ["Jan", "Feb", "Mar", "Apr", "May", "Jun"]
        axis_x = QBarCategoryAxis()
        axis_x.append(categories)
        chart.addAxis(axis_x, Qt.AlignBottom)
        bar_series.attachAxis(axis_x)

        axis_y = QValueAxis()
        chart.addAxis(axis_y, Qt.AlignLeft)
        bar_series.attachAxis(axis_y)
        axis_y.setRange(0, 20)

        chart.legend().setVisible(True)
        chart.legend().setAlignment(Qt.AlignBottom)

        self.chart_view = QChartView(chart)
        self.chart_view.setRenderHint(QPainter.Antialiasing)


if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow()
    window.showMaximized()
    sys.exit(app.exec_())
