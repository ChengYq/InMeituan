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
        # TODO: 这个地方,如果以后要增加入C离开C,就把position改变成[0,0,0,0]
        self.position = [0, 0]
        self.ite = 0  # 记录一张图上的点击次数
        self.firstWrite=True  # 记录是不是第一行写入的

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

        self.cid = self.fig.canvas.mpl_connect('button_press_event', self.onClick)
        self.bnext.on_clicked(self.newNext)  # functional programming!! is here



    # configure my chart plot
    def plotConfig(self):
        self.ax1.get_yaxis().get_major_formatter().set_useOffset(False)
        self.ax1.get_xaxis().get_major_formatter().set_useOffset(False)
        self.ax1.get_xaxis().get_major_formatter().set_scientific(False)
        self.ax2.get_yaxis().get_major_formatter().set_useOffset(False)
        self.ax2.get_xaxis().get_major_formatter().set_useOffset(False)
        self.ax2.get_xaxis().get_major_formatter().set_scientific(False)
        plt.subplots_adjust(bottom=0.2)


    def saveFile(self,path):
        s=open(path,'a+')
        if self.firstWrite==True:
            firstLine="bm_waybill_id"+"\t"+"click_arriveTime"+"\t"+"click_takeTime"+"\n"
            s.write(firstLine)
            self.firstWrite=False
        toWrite = str(wholeData[self.ind][7])+"\t"+str(int(self.position[0]))+"\t"+str(int(self.position[1]))+"\n"
        s.writelines(toWrite)


    def newNext(self,event):
        self.ind += 1
        xdata = wholeData[self.ind][2]
        ydata1 = wholeData[self.ind][1]
        ydata2 = wholeData[self.ind][0]
        print wholeData[self.ind][7]


        self.l1.set_ydata(ydata1)
        self.l1.set_xdata(xdata)
        self.l2.set_ydata(ydata2)
        self.l2.set_xdata(xdata)
        self.ax1.relim()
        self.ax1.autoscale_view()
        self.ax2.relim()
        self.ax2.autoscale_view()
        plt.draw()

        print "sdf"

        self.clickArriveUp.remove()
        self.clickArriveDown.remove()
        self.clickTakeUp.remove()
        self.clickTakeDown.remove()

        plt.draw()

# TODO:输出的目标文件,根据需要修改路径。该文件必须不存在(程序会自动创建),否则会修改已有的文件!!!
        self.saveFile('/Users/chyq/Desktop/res.txt')


    # this method defines what to do when you click on the plot
    def onClick(self, event):
        clk_x = event.xdata
        if clk_x>2:
            self.ite += 1
            print self.ite
            if self.ite%3==1:
                self.position[0] = clk_x
                self.clickArriveUp=self.ax1.axvline(x=self.position[0])
                self.clickArriveDown = self.ax2.axvline(x=self.position[0])

                self.clickArriveUp.set_color('g')
                self.clickArriveUp.set_linewidth(2)
                self.clickArriveDown.set_color('g')
                self.clickArriveDown.set_linewidth(2)

                plt.draw()
            elif self.ite%3==2:
                self.position[1] = clk_x
                self.clickTakeUp=self.ax1.axvline(x=self.position[1])
                self.clickTakeDown=self.ax2.axvline(x=self.position[1])

                self.clickTakeUp.set_color('k')
                self.clickTakeUp.set_linewidth(2)
                self.clickTakeDown.set_color('k')
                self.clickTakeDown.set_linewidth(2)

                plt.draw()
            else: # 在画图区域点了第三次的时候触发的动作
                self.position=[0,0]
                self.clickArriveUp.remove()
                self.clickArriveDown.remove()
                self.clickTakeUp.remove()
                self.clickTakeDown.remove()
                plt.draw()
        else:# 点击在了Next button 上
            self.ite =0

        print self.position



# 这个是新版本的main函数
if __name__ == '__main__':
    e = EvaluationTool()
    # TODO:源数据路径,根据需要修改
    e.generateData("/Users/chyq/Desktop/dataSet2.txt")
    e.drawPlot()
    e.plotConfig()
    plt.show()
