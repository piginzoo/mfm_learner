# 说明

数据源，现在市面上靠谱的就这几家：
- [tushare](https://tushare.pro): 200元就可以搞定，还挺好用的，主要用它，目前。基金数据补全，别的还好。
- [baostock](http://www.baostock.com):不是特别全，好处是不用登陆，完全免费。没有基金数据。
- [akshare](https://www.akshare.xyz): 我理解都是wrapper的爬虫，完全免费，数据很全。有基金数据。

# 设计

做了个抽象类，然后子类来实现，有个工厂类返回实例，根据 [conf/config.yml](../conf/config.yml)中的`datasource`配置。

实现了一个cache。