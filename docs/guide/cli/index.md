---
title: Command Line Interface
rank: 10
---

# Command Line Interface

The Zelox command line interface (CLI) provides commands for interacting with Zelox from the terminal.
You can start and manage Zelox servers, run PySpark scripts, and more.

## One-Shot Execution

You can use the `zelox spark run` command to run any PySpark script without explicitly provisioning a server.
A local Zelox server starts instantly when you run the command and automatically stops when the script finishes.

The script can access the Spark session through the `spark` variable, which connects to the local Zelox server using the Spark Connect protocol.

### Piping a Script

You can pipe simple PySpark code to the `zelox spark run` command directly.

```bash
echo 'spark.sql("SELECT 1 + 1").show()' | zelox spark run
```

### Using a Heredoc

For more complex scripts, you can use a heredoc.

```bash
cat <<EOF | zelox spark run
import pyspark.sql.functions as F

df = spark.createDataFrame([(1, 2), (2, 3)], ["a", "b"])
df = df.withColumn("sum", F.col("a") + F.col("b"))
df.show()
EOF
```

### Running a Script File

You can also write the PySpark script to a file and run it by specifying the file path with the `-f` option.

```bash
zelox spark run -f script.py
```

### Using with Agents

The `zelox spark run` command can also be exposed as an agent skill, enabling LLM agents to perform data processing tasks using PySpark.
See the [Agent Skills](/guide/integrations/agent-skills) page for more details.

## Spark Connect Server

You can start a Spark Connect server that uses Zelox for computation.

```bash
zelox spark server --ip 127.0.0.1 --port 50051
```

## PySpark Shell

You can start an interactive PySpark shell that uses Zelox for computation.

```bash
zelox spark shell
```

## Arrow Flight SQL Server

You can run Zelox as an Arrow Flight SQL server.

```bash
zelox flight server --ip 127.0.0.1 --port 32010
```

More details can be found on the [Arrow Flight SQL](/guide/integrations/flight-sql) page.
