echo "开始准备运行环境:"

__venv_dir="./.venv"

__python3=`which python3`
if [ "$__python3" = "" ];then
    echo "\t没有python3环境，安装失败!!!"
    exit
fi


__virtualenv=`which virtualenv`
if [ "$__virtualenv" = "" ];then
    echo "\t请先安装virtualenv，参考：https://www.liaoxuefeng.com/wiki/1016959663602400/1019273143120480"
    exit
fi

if [ ! -d  ];then
  echo "\t创建虚拟环境：$__venv_dir"
  mkdir $__venv_dir
fi

virtualenv -p $__python3 $__venv_dir
echo "\t安装virtualenv环境[python:$__python3]成功，virtualenv虚拟环境位于:$__venv_dir"

echo "\t激活虚拟环境"
source $__venv_dir/bin/activate

echo "\t安装所有依赖的pip包："
pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/
ARCHFLAGS="-arch x86_64" pip install -r requirement.txt

echo "\t安装魔改的alphalens：https://gitee.com/piginzoo/alphalens"
git clone --depth 1 https://gitee.com/piginzoo/alphalens.git $__venv_dir/alphalens
cd $__venv_dir/alphalens
python setup.py install
if [ $? -ne 0 ]; then
  echo "\t安装alphalens失败!!!"
  return
fi

cd ../..

echo "\t安装魔改的jaqs：https://gitee.com/piginzoo/jaqs-fxdayu"
git clone --depth 1 https://gitee.com/piginzoo/jaqs-fxdayu.git $__venv_dir/alphalens
cd $__venv_dir/alphalens
python setup.py install
if [ $? -ne 0 ]; then
  echo "\t安装jaqs_fxdayu失败!!!"
  return
fi

cd ../..

echo "环境准备成功完成，请参阅：README.md，开始运行程序吧!"
