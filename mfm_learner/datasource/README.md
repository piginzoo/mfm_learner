# 说明

数据源，现在市面上靠谱的就这几家：
- [tushare](https://tushare.pro): 200元就可以搞定，还挺好用的，主要用它，目前。基金数据补全，别的还好。
- [baostock](http://www.baostock.com):不是特别全，好处是不用登陆，完全免费。没有基金数据。
- [akshare](https://www.akshare.xyz): 我理解都是wrapper的爬虫，完全免费，数据很全。有基金数据。

目前主要还是用的tushare和database，tushare只是用作简单测试，
大数据量还是得用database，即把tushare的数据离线下载到数据中，
目前，接口设计成一致的，但是，完全实现的就是上述的tushare和database。

关于tushare的数据下载，可以参考[Tushare数据下载](../utils/tushare_download/README.md)。

# 设计

做了个抽象类，然后子类来实现，有个工厂类返回实例，根据 [conf/config.yml](../conf/config.yml)中的`datasource`配置。

使用很简单:`datasource_factor.get()`即可，得到datasource的实例，然后按照接口调用即可。

- 实现了tushare的缓存

    使用@cache的标注，来自动实现了tushare中，调用后的缓存；如果再调用，会自动加载缓存。

- 实现了数据加载时候的列名rename

    使用了@post_query的标注，来自动实现了列名的修改，可以帮助统一列名


