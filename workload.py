import matplotlib.pyplot as plt
import numpy as np
aditya = np.loadtxt('CDF_aditya.txt')
dctcp = np.loadtxt('CDF_dctcp.txt')
datamining = np.loadtxt('CDF_datamining.txt')

plt.plot(aditya[:,0]*1500,aditya[:,2],color='red',label='IMC10',linewidth=3)
plt.plot(dctcp[:,0]*1500,dctcp[:,2],color='blue',label='Web Search',linewidth=3)
plt.plot(datamining[:,0]*1500,datamining[:,2],color='green',label='Datamining',linewidth=3)
plt.savefig('workloads.eps',format='eps')
plt.grid(True)
plt.legend(fontsize=20)
plt.xscale('log')
plt.tick_params(axis='both', which='major', labelsize=13)
plt.xlabel('Flow Size (Bytes)',fontsize=20)
plt.ylabel('CDF',fontsize=20)
plt.tight_layout()
plt.savefig('workloads.eps',format='eps')