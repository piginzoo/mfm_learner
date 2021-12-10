# cache就在当前文件的目录，创建的一个cache.json目录
def __get_cache_name(url, md5):
    url = url.replace("http://", "")
    url = url.replace("/", "_")
    cache_dir = os.path.join(conf.root_path, "data", "http_cache", url)
    if not os.path.exists(cache_dir):
        logger.info("缓存目录[%s]不存在，创建！", cache_dir)
        os.makedirs(cache_dir)

    # 确定json的cache文件名
    json_path = os.path.join(cache_dir, md5 + ".json")
    return json_path


def __load_cache(url, md5):
    json_path = __get_cache_name(url, md5)

    if os.path.exists(json_path):
        logger.debug("缓存文件[%s]存在，加载之", json_path)
        with open(json_path, "r", encoding="utf-8") as f:
            j_data = f.read()
            j_data = j_data.replace("'", '"')
            data = json.loads(j_data)
            return data
    else:
        logger.debug("加载缓存失败，缓存文件[%s]不存在", json_path)
        return None


def __cache_me(url, result_data, md5):
    if result_data is None:
        logger.warning("返回异常或者错误码不为0，不缓存")
        return

    # 错误码不为0，不缓存
    if result_data.get('code', None) and result_data['code'] != '0':
        # logger.debug("结果为空或调用未成功，不缓存")
        return

    json_path = __get_cache_name(url, md5)
    with open(json_path, "w") as jf:
        jf.write(str(result_data))
        logger.debug("[url]调用缓存到:%s", json_path)

