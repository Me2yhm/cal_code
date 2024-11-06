import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time


class DirectoryWatcher(FileSystemEventHandler):
    def __init__(self, directory):
        self.directory = directory
        self.file_list = self.get_file_list()

    def get_file_list(self):
        """获取当前目录下的文件列表"""
        return set(os.listdir(self.directory))

    def on_created(self, event):
        """当文件被创建时调用"""
        if not event.is_directory:
            print(f"文件创建: {event.src_path}")
            self.file_list.add(os.path.basename(event.src_path))
            self.print_file_list()

    def on_deleted(self, event):
        """当文件被删除时调用"""
        if not event.is_directory:
            print(f"文件删除: {event.src_path}")
            self.file_list.discard(os.path.basename(event.src_path))
            self.print_file_list()

    def print_file_list(self):
        """打印当前文件列表"""
        print("当前文件列表:", self.file_list)


def main(directory):
    event_handler = DirectoryWatcher(directory)
    observer = Observer()
    observer.schedule(event_handler, directory, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)  # 让主线程保持运行
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    directory_to_watch = "/path/to/your/directory"  # 替换为你要监控的目录
    main(directory_to_watch)
