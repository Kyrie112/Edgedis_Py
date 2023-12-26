# # 下载Python源码压缩包
# wget https://www.python.org/ftp/python/3.8.18/Python-3.8.18.tar.xz

# 解压缩
tar -xvf Python-3.8.18.tar.xz

# 进入解压后的目录
cd Python-3.8.18

# 配置和编译
./configure
make

# 安装Python
sudo make install

# 验证安装
python3 --version