# coding=utf-8

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.widgets import Button
import matplotlib.ticker as ticker
import copy
import numpy as np
from matplotlib.widgets import Button

## global variables

wholeData = []


class EvaluationTool():
    # initialize the class
    def __init__(self):
        self.l1 = []
        self.timeSe = []
        self.speedSe = []
        self.distSe = []
        self.waybillID = 0
        self.takeTime = 0
        self.arriveTime = 0
        self.in_time = 0
        self.out_time = 0
        self.dataSet = [[] for i in range(8)]
        self.ind = 0  # 用来进行dataSet索引的


    # using this method to generate dataSet
    def generateData(self, path):
        f = open(path)
        global wholeData
        lineContext = f.readlines()
        count = len(lineContext)

        for i in range(count):
            oneLine = lineContext[i].split()
            if i == 0:
                continue
            elif i == 1:
                self.waybillID = oneLine[0]
            if oneLine[0] == self.waybillID:
                self.waybillID = oneLine[0]
                self.takeTime = long(oneLine[1])
                self.arriveTime = long(oneLine[2])
                self.speed = float(oneLine[3])
                self.dist = float(oneLine[4])
                self.time = long(oneLine[5])
                self.in_time = long(oneLine[6])
                self.out_time = long(oneLine[7])
                self.timeSe.append(self.time)
                self.speedSe.append(self.speed)
                self.distSe.append(self.dist)
            else:
                self.dataSet[0] = self.distSe
                self.dataSet[1] = self.speedSe
                self.dataSet[2] = self.timeSe
                self.dataSet[3] = self.takeTime
                self.dataSet[4] = self.arriveTime
                self.dataSet[5] = self.in_time
                self.dataSet[6] = self.out_time
                self.dataSet[7] = self.waybillID
                temp = copy.deepcopy(self.dataSet)
                wholeData.append(temp)

                self.timeSe = []
                self.speedSe = []
                self.distSe = []
                self.takeTime = 0
                self.arriveTime = 0
                self.in_time = 0
                self.out_time = 0
                ## above is to handle old dataSets
                self.waybillID = oneLine[0]
                self.takeTime = long(oneLine[1])
                self.arriveTime = long(oneLine[2])
                self.speed = float(oneLine[3])
                self.dist = float(oneLine[4])
                self.time = long(oneLine[5])
                self.in_time = long(oneLine[6])
                self.out_time = long(oneLine[7])
                self.timeSe.append(self.time)
                self.speedSe.append(self.speed)
                self.distSe.append(self.dist)

    # draw chart plot
    def drawPlot(self):
        self.fig, (self.ax1, self.ax2) = plt.subplots(2, 1)
        self.l1, = self.ax1.plot(list(wholeData[0][2]), list(wholeData[0][1]))
        self.l2, = self.ax2.plot(list(wholeData[0][2]), list(wholeData[0][0]))
        self.ax1.set_title('Speed')
        self.ax2.set_title("Distance")
        self.axnext = plt.axes([0.81, 0.05, 0.1, 0.075])
        self.bnext = Button(self.axnext, 'Next')
        self.bnext.on_clicked(self.next)  # functional programming!! is here

    # draw vertical lines
    def drawVerticalLine(self, ind):
        self.a11 = self.ax1.axvline(x=wholeData[ind][3])
        self.a12 = self.ax1.axvline(x=wholeData[ind][4])
        self.a13 = self.ax1.axvline(x=wholeData[ind][5])
        self.a14 = self.ax1.axvline(x=wholeData[ind][6])

        self.a21 = self.ax2.axvline(x=wholeData[ind][3])
        self.a22 = self.ax2.axvline(x=wholeData[ind][4])
        self.a23 = self.ax2.axvline(x=wholeData[ind][5])
        self.a24 = self.ax2.axvline(x=wholeData[ind][6])

    # configure my chart plot
    def plotConfig(self):
        self.ax1.get_yaxis().get_major_formatter().set_useOffset(False)
        self.ax1.get_xaxis().get_major_formatter().set_useOffset(False)
        self.ax1.get_xaxis().get_major_formatter().set_scientific(False)
        self.ax2.get_yaxis().get_major_formatter().set_useOffset(False)
        self.ax2.get_xaxis().get_major_formatter().set_useOffset(False)
        self.ax2.get_xaxis().get_major_formatter().set_scientific(False)
        plt.subplots_adjust(bottom=0.2)

    # vertical line in two graphs, each graph has 4 lines,they are a,b,c,d
    def verticalConfig(self, a, b, c, d):
        a.set_color('g')
        a.set_linewidth(2)
        b.set_color('g')
        b.set_linewidth(2)
        c.set_color('r')
        c.set_linewidth(2)
        d.set_color('r')
        d.set_linewidth(2)

    # delete vertical line
    def removeVertical(self):
        self.a12.remove()
        self.a11.remove()
        self.a13.remove()
        self.a14.remove()
        self.a21.remove()
        self.a22.remove()
        self.a23.remove()
        self.a24.remove()

    # define what does "Next" Button do
    def next(self, event):
        self.ind += 1
        xdata = wholeData[self.ind][2]
        ydata1 = wholeData[self.ind][1]
        ydata2 = wholeData[self.ind][0]
        print wholeData[self.ind][7]

        self.removeVertical()
        self.drawVerticalLine(self.ind)
        self.verticalConfig(self.a11, self.a12, self.a13, self.a14)
        self.verticalConfig(self.a21, self.a22, self.a23, self.a24)

        self.l1.set_ydata(ydata1)
        self.l1.set_xdata(xdata)
        self.l2.set_ydata(ydata2)
        self.l2.set_xdata(xdata)
        self.ax1.relim()
        self.ax1.autoscale_view()
        self.ax2.relim()
        self.ax2.autoscale_view()
        plt.draw()



if __name__=='__main__':
    e=EvaluationTool()
    e.generateData('/Users/chyq/Desktop/dataSet2.txt')
    e.drawPlot()
    e.plotConfig()
    e.drawVerticalLine(0)
    e.verticalConfig(e.a11,e.a12,e.a13,e.a14)
    e.verticalConfig(e.a21,e.a22,e.a23,e.a24)
    plt.show()




