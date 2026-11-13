import os


class Config:
    """数据湖存储布局：
    DATA_DIR/{catalog.db, parquet/, staging/, dicts/, locks/}
    """

    def __init__(self, data_dir=None):
        base = data_dir or os.environ.get("DATA_DIR") or os.path.join(os.getcwd(), "data")
        self.data_dir = os.path.abspath(base)
        self.db_path = os.path.join(self.data_dir, "catalog.db")
        self.parquet_dir = os.path.join(self.data_dir, "parquet")
        self.staging_dir = os.path.join(self.data_dir, "staging")
        # 鸟种别名字典（不可变 JSON 文件，每版一个）
        self.dict_dir = os.path.join(self.data_dir, "dicts")
        # 跨进程互斥锁（替换全程持有的排他 flock 文件等）
        self.lock_dir = os.path.join(self.data_dir, "locks")
