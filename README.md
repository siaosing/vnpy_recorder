# vnpy_recorder
record market data for futures listed in mainland China based on vnpy components

借助于vnpy_datarecorder模块和redis数据库，将期货tick数据实时录入内存，利用redis的定时落盘机制将数据缓存至硬盘，交易结束后将数据
转存为csv文件并压缩打包存放。

dependency:
vnpy
vnpy_ctp
vnpy_datarecorder
python 3.10(建议直接使用VeighNa Studio发行版)
Windows Redis(安装后加入系统服务)
