#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import time
from PyQt5.QtWidgets import (QApplication, QWidget, QVBoxLayout,
                             QLabel, QPushButton)
from PyQt5.QtCore import QThread, pyqtSignal


class WorkerThread(QThread):
    trigger = pyqtSignal(str)
    finished = pyqtSignal()

    def __init__(self):
        super().__init__()

    def run(self):
        for i in range(5):
            time.sleep(1)
            self.trigger.emit(str(i+1))
            print('WorkerThread::run ' + str(i))
        self.finished.emit()


class MyWidget(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.setWindowTitle('my window')
        self.setGeometry(50, 50, 200, 150)

        layout = QVBoxLayout()
        self.setLayout(layout)

        self.mylabel = QLabel('press button to start thread', self)
        layout.addWidget(self.mylabel)

        self.mybutton = QPushButton('start', self)
        self.mybutton.clicked.connect(self.startThread)
        layout.addWidget(self.mybutton)

        self.work = WorkerThread()

    def startThread(self):
        self.mybutton.setDisabled(True)
        self.work.start()
        self.work.trigger.connect(self.updateLabel)
        self.work.finished.connect(self.threadFinished)
        self.updateLabel(str(0))

    def threadFinished(self):
        self.mybutton.setDisabled(False)

    def updateLabel(self, text):
        self.mylabel.setText(text)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    w = MyWidget()
    w.show()
    sys.exit(app.exec_())