## 安晴网的一个图片自动转存到阿里云oss的插件

由于资金问题，买不起服务器，所以使用虚拟机主机+oss模式

思路

1、取数据库中视频数据最后id

2、将该id写入到history表做标记

3、依次下载并更新数据库

4、定时执行