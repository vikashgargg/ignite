import sys
from pathlib import Path

from pyspark.sql import SparkSession


def read_script(file):
    if file == "-":
        return (sys.stdin.read(), "<stdin>")

    path = Path(file)
    if not path.is_absolute():
        path = Path.cwd() / path
    path = path.resolve()

    return (path.read_text(), str(path))


def run_pyspark_script(port, file):
    source, filename = read_script(file)
    spark = SparkSession.builder.remote("sc://localhost:%d" % port).getOrCreate()
    scope = {
        "__name__": "__main__",
        "__file__": filename,
        "__package__": None,
        "spark": spark,
    }
    try:
        exec(compile(source, filename, "exec"), scope)  # noqa: S102
    finally:
        spark.stop()


if __name__ == "__main__":
    run_pyspark_script(int(sys.argv[1]), sys.argv[2])
