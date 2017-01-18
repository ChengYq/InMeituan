import numpy as np
import matplotlib.pyplot as plt
from matplotlib.widgets import Button
import matplotlib.ticker as ticker
import copy
import numpy as np
from matplotlib.widgets import Button

f=open('/Users/chyq/Desktop/dataSet2.txt')
lineContext=f.readlines()
count=len(lineContext)
timeSe=[]
speedSe=[]
distSe=[]
waybillID=0
takeTime=0
arriveTime=0
in_time=0
out_time=0
dataSet = [[]for i in range(8)]
wholeData=[]
for i in range(count):
    oneLine = lineContext[i].split()
    if i==0:
        continue
    elif i==1:
        waybillID=oneLine[0]
    if oneLine[0]==waybillID:
        waybillID=oneLine[0]
        takeTime=long(oneLine[1])
        arriveTime=long(oneLine[2])
        speed=float(oneLine[3])
        dist=float(oneLine[4])
        time=long(oneLine[5])
        in_time = long(oneLine[6])
        out_time = long(oneLine[7])
        timeSe.append(time)
        speedSe.append(speed)
        distSe.append(dist)
    else:
        dataSet[0]=distSe
        dataSet[1]=speedSe
        dataSet[2]=timeSe
        dataSet[3]=takeTime
        dataSet[4]=arriveTime
        dataSet[5]= in_time
        dataSet[6]=out_time
        dataSet[7]=waybillID
        temp=copy.deepcopy(dataSet)
        wholeData.append(temp)

        timeSe=[]
        speedSe=[]
        distSe=[]
        takeTime=0
        arriveTime=0
        in_time=0
        out_time=0
        ## above is to handle old dataSets
        waybillID=oneLine[0]
        takeTime=long(oneLine[1])
        arriveTime=long(oneLine[2])
        speed=float(oneLine[3])
        dist=float(oneLine[4])
        time=long(oneLine[5])
        in_time = long(oneLine[6])
        out_time = long(oneLine[7])
        timeSe.append(time)
        speedSe.append(speed)
        distSe.append(dist)



fig,(ax1,ax2)=plt.subplots(2,1)
l1, = ax1.plot(list(wholeData[0][2]),list(wholeData[0][1]))
ax1.set_title('Speed')
ax2.set_title("Distance")

a11=ax1.axvline(x=wholeData[0][3])
a12=ax1.axvline(x=wholeData[0][4])
a13=ax1.axvline(x=wholeData[0][5])
a14=ax1.axvline(x=wholeData[0][6])
a11.set_color('g')
a11.set_linewidth(2)
a12.set_color('g')
a12.set_linewidth(2)
a13.set_color('r')
a13.set_linewidth(2)
a14.set_color('r')
a14.set_linewidth(2)



l2, = ax2.plot(list(wholeData[0][2]),list(wholeData[0][0]))
a21=ax2.axvline(x=wholeData[0][3])
a22=ax2.axvline(x=wholeData[0][4])
a23=ax2.axvline(x=wholeData[0][5])
a24=ax2.axvline(x=wholeData[0][6])
a21.set_color('g')
a21.set_linewidth(2)
a22.set_color('g')
a22.set_linewidth(2)
a23.set_color('r')
a23.set_linewidth(2)
a24.set_color('r')
a24.set_linewidth(2)

print waybillID



ax1.get_yaxis().get_major_formatter().set_useOffset(False)
ax1.get_xaxis().get_major_formatter().set_useOffset(False)

ax2.get_yaxis().get_major_formatter().set_useOffset(False)
ax2.get_xaxis().get_major_formatter().set_useOffset(False)


plt.subplots_adjust(bottom=0.2)

class Index(object):
    ind = 0
    a = None
    b=None
    c=None
    d=None
    e=None
    f=None
    g=None
    h=None


    def next(self, event):
        self.ind += 1

        xdata=wholeData[self.ind][2]
        ydata1=wholeData[self.ind][1]
        ydata2=wholeData[self.ind][0]
        print wholeData[self.ind][7]


        if self.ind==1:
            a12.remove()
            a11.remove()
            a13.remove()
            a14.remove()
            a21.remove()
            a22.remove()
            a23.remove()
            a24.remove()
        else:
            self.a.remove()
            self.b.remove()
            self.c.remove()
            self.d.remove()
            self.e.remove()
            self.f.remove()
            self.g.remove()
            self.h.remove()



        self.a=ax1.axvline(x=wholeData[self.ind][3])
        self.b=ax1.axvline(x=wholeData[self.ind][4])
        self.e=ax1.axvline(x=wholeData[self.ind][5])
        self.f=ax1.axvline(x=wholeData[self.ind][6])
        self.a.set_color('g')
        self.a.set_linewidth(2)
        self.b.set_color('g')
        self.b.set_linewidth(2)
        self.e.set_color('r')
        self.e.set_linewidth(2)
        self.f.set_color('r')
        self.f.set_linewidth(2)


        self.c= ax2.axvline(x=wholeData[self.ind][3])
        self.d= ax2.axvline(x=wholeData[self.ind][4])
        self.g= ax2.axvline(x=wholeData[self.ind][5])
        self.h = ax2.axvline(x=wholeData[self.ind][6])
        self.c.set_color('g')
        self.c.set_linewidth(2)
        self.d.set_color('g')
        self.d.set_linewidth(2)
        self.g.set_color('r')
        self.g.set_linewidth(2)
        self.h.set_color('r')
        self.h.set_linewidth(2)





        l1.set_ydata(ydata1)
        l1.set_xdata(xdata)



        l2.set_ydata(ydata2)
        l2.set_xdata(xdata)

        # recompute the ax.dataLim
        ax1.relim()
        # update ax.viewLim using the new dataLim
        ax1.autoscale_view()

        # recompute the ax.dataLim
        ax2.relim()
        # update ax.viewLim using the new dataLim
        ax2.autoscale_view()

        plt.draw()



    # def prev(self, event):
    #     self.ind -= 1
    #     i = self.ind % len(freqs)
    #     ydata = np.sin(2*np.pi*freqs[i]*t)
    #     l.set_ydata(ydata)
    #     plt.draw()

callback = Index()
# axprev = plt.axes([0.7, 0.05, 0.1, 0.075])
axnext = plt.axes([0.81, 0.05, 0.1, 0.075])
bnext = Button(axnext, 'Next')
bnext.on_clicked(callback.next)
# bprev = Button(axprev, 'Previous')
# bprev.on_clicked(callback.prev)

plt.show()


