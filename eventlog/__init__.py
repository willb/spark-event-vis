import pyspark
import pyspark.sql.functions as F
from pyspark import SparkConf

import json
from collections import namedtuple

appmeta = None
sparkSession = None
options = dict()

def init_eventlog(df, **kwargs):
    global sparkSession, options
    sparkSession = df.sql_ctx.sparkSession
    for k, v in kwargs.items():
        options[k] = v
    with_appmeta(df)

def with_appmeta(df):
    global appmeta

    if appmeta is None:
        appmeta = df.select("App ID", "App Name").dropna().collect()[0]
    
    app_id, app_name = appmeta
    return df.withColumn("Application ID", F.lit(app_id)).withColumn("Application Name", F.lit(app_name))

def session_from_df(df):
    return df.sql_ctx.sparkSession

def raw_job_info(df):
    starts = df.where(F.col("Event") == "SparkListenerJobStart").select('Job ID', 'Submission Time', 'Stage IDs', 'Stage Infos', 'Properties')
    ends = df.where(F.col("Event") == "SparkListenerJobEnd").select('Job ID', 'Completion Time', 'Job Result.Result')

    jobs = starts.join(ends, "Job ID").withColumn("JobDuration", F.col("Completion Time") - F.col("Submission Time")).withColumn("Submission Date", F.from_unixtime(F.col("Submission Time") / 1000))
    return jobs

def all_stage_meta(df):
    jobs = raw_job_info(df)
    allmeta = jobs.select('Job ID', F.explode('Stage Infos').alias('Stage Info')).select('Job ID', 'Stage Info.*')
    return allmeta

def stage_meta(df):
    return all_stage_meta(df).drop('Parent IDs', 'RDD Info', 'Accumulables')

def stage_parents(df):
    return all_stage_meta(df).select('Job ID', 'Stage ID', F.explode('Parent IDs').alias('Parent ID'))
    
def stage_rddinfo(df):
    rddinfo = all_stage_meta(df).select('Job ID', 'Stage ID', F.explode('RDD Info').alias('RDD Info')).select('Job ID', 'Stage ID', 'RDD Info.*')
    return rddinfo

def stage_rddmeta(df):
    return stage_rddinfo(df).select('*', 'Storage Level.*').drop('Storage Level', 'Parent IDs')

def stage_rddparents(df):
    return stage_rddinfo(df).select('Job ID', 'Stage ID', 'RDD ID', F.explode('Parent IDs').alias('RDD Parent ID'))

def job_info(df):
    return raw_job_info(df).drop('Stage IDs', 'Stage Infos', 'Properties')

def collect_and_dictify(df):
    return [json.loads(row[0]) for row in df.selectExpr("to_json(*)").collect()]

def executor_info(df):
    info = df.select("Executor Info").dropna()
    return collect_and_dictify(info)

def plan_dicts(df):
    return collect_and_dictify(df.select("sparkPlanInfo").dropna())

MetricNode = namedtuple("MetricNode", "plan_node accumulatorId metricType name")
PlanInfoNode = namedtuple("PlanInfoNode", "plan_node parent nodeName simpleString")

def nextid():
    i = 0
    while True:
        yield i
        i = i + 1
    
node_ctr = nextid()

def plan_dicts(df):
    return collect_and_dictify(df.select("sparkPlanInfo").dropna())

def flatplan(dicts, parent=-1, plan_nodes=None, metric_nodes=None):
    if plan_nodes is None:
        plan_nodes = list()
        
    if metric_nodes is None:
        metric_nodes = list()
    
    for pd in dicts:
        pid = next(node_ctr)
        for m in pd['metrics']:
            metric_nodes.append(MetricNode(pid, m['accumulatorId'], m['metricType'], m['name']))
        
        plan_nodes.append(PlanInfoNode(pid, parent, pd['nodeName'], pd['simpleString']))
        
        flatplan(pd['children'], pid, plan_nodes, metric_nodes)
    
    return(plan_nodes, metric_nodes)

def plan_dfs(df):
    spark = session_from_df(df)
    pn, mn = flatplan(plan_dicts(df))
    
    pndf = with_appmeta(spark.createDataFrame(data=pn))
    mndf = with_appmeta(spark.createDataFrame(data=mn))
    
    return (pndf, mndf)

def tasks_to_stages(df):
    return df.where(F.col('Event') == 'SparkListenerTaskStart').select(F.col("Task Info.Task ID").alias('Task ID'), 'Stage ID')


def accumulables(df, noun='Task', extra_cols=[]):
    mcol = '%s Info' % noun
    idcol = '%s ID' % noun
    
    acc_cols = [F.col('Accumulable.%s' % s).alias('Metric %s' % s) for s in ['ID', 'Name', 'Value']]
    obs = df.select(mcol, *extra_cols).select('%s.*' % mcol, *extra_cols)
    cols = [F.col(elt) for elt in sorted(set(obs.columns) - set([idcol, 'Accumulables']))]
    
    return obs.select(
        idcol, 
        F.explode('Accumulables').alias('Accumulable'), 
        *(cols)
    ).select(
        idcol, 
        *(cols + acc_cols)
    ).withColumnRenamed("Metric ID", "accumulatorId").withColumn("Metric Value", F.col("Metric Value").cast("float"))

def explicit_task_metrics(df, noun="Task"):
    def flatten_once(df):
        def metric_alias(column, subfield):
            if 'transform_structure_prefixes' in options and options['transform_structure_prefixes']:
                raw = "%s %s" % (column.name, subfield.name)
                seen = set()
                result = []
                for token in raw.split():
                    if not token in seen:
                        result.append(token)
                    seen.add(token)
                return " ".join(result)
            elif 'use_structure_prefixes' in options and options['use_structure_prefixes']:
                return "%s %s" % (column.name, subfield.name)
            else:
                return subfield.name
            
        import pyspark.sql.types as T

        cols = []
    
        for column in df.schema:
            if isinstance(column.dataType, T.StructType):
                for subfield in column.dataType:
                    cols.append(F.col("%s.%s" % (column.name, subfield.name)).alias(metric_alias(column, subfield)))
            elif isinstance(column.dataType, T.ArrayType):
                pass
            else:
                cols.append(F.col(column.name))

        return df.select(*cols)
    
    mcol = '%s Info' % noun
    idcol = '%s ID' % noun
    
    obs = df.select('%s.*' % mcol, "Stage ID", F.col("Task Metrics.*"), F.col("Task Executor Metrics.*"))
    cols = [F.col(elt) for elt in obs.columns if elt not in set([idcol, 'Accumulables'])]

    flat_df = flatten_once(obs.select(idcol, *cols))
    
    common_split = flat_df.columns.index("Stage ID") + 1
    
    common_columns = flat_df.columns[:common_split]
    metric_columns = flat_df.columns[common_split:]
    
    result = flat_df.where(
        F.lit(False)
    ).select(
        *(common_columns + 
          [
              F.lit(None).alias("accumulatorID"), 
              F.lit(None).alias("Metric Name"), 
              F.lit(None).alias("Metric Value")
          ]
         )
    )
    
    for col in metric_columns:
        result = result.union(
            flat_df.select(
                *(
                    common_columns + 
                    [
                        F.lit(None).alias("accumulatorID"), 
                        F.lit(col).alias("Metric Name"), 
                        F.col(col).alias("Metric Value")
                    ]
                )
            )
        )
    
    return result

def tidy_metrics(df, noun='Task', event=None, interesting_metrics=None, extra_cols=[]):
    mcol = '%s Info' % noun
    idcol = '%s ID' % noun
    
    if event is not None:
        event_selector = (F.col('Event') == event)
    else:
        event_selector = F.lit(True)
    
    filtered = df.where(event_selector)
    
    return with_appmeta(accumulables(filtered, noun, extra_cols).unionByName(explicit_task_metrics(filtered)))

def tidy_tasks(df, event='SparkListenerTaskEnd', interesting_metrics=None):
    return tidy_metrics(df, 'Task', event=event, interesting_metrics=(interesting_metrics or F.lit(True)), extra_cols=["Stage ID"])

def tidy_stages(df, event='SparkListenerStageCompleted', interesting_metrics=None):
    return tidy_metrics(df, 'Stage', event=event, interesting_metrics=F.lit(True))

def split_metrics(df):
    metric_columns = set(["Metric Name", "Metric Value", "accumulatorId", "kind", "unit"])
    common_columns = set(["Application ID", "Application Name", "Task ID", "Stage ID"])
    
    metrics = df.select(*[col for col in df.columns if col in metric_columns or col in common_columns])
    task_meta = df.drop(*metric_columns).distinct()
    
    return (metrics, task_meta)



MetricMeta = namedtuple('MetricMeta', 'MetricName kind unit')

metric_metas = [
    MetricMeta('GPU decode time', 'time', 'ns'),
    MetricMeta('GPU time', 'time', 'ns'),
    MetricMeta('avg hash probe bucket list iters', 'count', 'iterations'),
    MetricMeta('buffer time', 'time', 'ns'),
    MetricMeta('build side size', 'size', 'bytes'),
    MetricMeta('build time', 'time', 'ms'),
    MetricMeta('collect batch time', 'time', 'ns'),
    MetricMeta('concat batch time', 'time', 'ns'),
    MetricMeta('data size', 'size', 'bytes'),
    MetricMeta('duration', 'time', 'ms'),
    MetricMeta('fetch wait time', 'time', 'ms'),
    MetricMeta('internal.metrics.diskBytesSpilled', 'size', 'bytes'),
    MetricMeta('internal.metrics.executorCpuTime', 'time', 'ns'),
    MetricMeta('internal.metrics.executorDeserializeCpuTime', 'time', 'ns'),
    MetricMeta('internal.metrics.executorDeserializeTime', 'time', 'ms'),
    MetricMeta('internal.metrics.executorRunTime', 'time', 'ms'),
    MetricMeta('internal.metrics.input.bytesRead', 'size', 'bytes'),
    MetricMeta('internal.metrics.input.recordsRead', 'count', 'records'),
    MetricMeta('internal.metrics.jvmGCTime', 'time', 'ms'),
    MetricMeta('internal.metrics.memoryBytesSpilled', 'size', 'bytes'),
    MetricMeta('internal.metrics.output.bytesWritten', 'size', 'bytes'),
    MetricMeta('internal.metrics.output.recordsWritten', 'count', 'records'),
    MetricMeta('internal.metrics.peakExecutionMemory', 'size', 'bytes'),
    MetricMeta('internal.metrics.resultSerializationTime', 'time', 'ms'),
    MetricMeta('internal.metrics.resultSize', 'size', 'bytes'),
    MetricMeta('internal.metrics.shuffle.read.fetchWaitTime', 'time', 'ms'),
    MetricMeta('internal.metrics.shuffle.read.localBlocksFetched', 'count', 'blocks'),
    MetricMeta('internal.metrics.shuffle.read.localBytesRead', 'size', 'bytes'),
    MetricMeta('internal.metrics.shuffle.read.recordsRead', 'count', 'records'),
    MetricMeta('internal.metrics.shuffle.read.remoteBlocksFetched', 'count', 'blocks'),
    MetricMeta('internal.metrics.shuffle.read.remoteBytesRead', 'size', 'bytes'),
    MetricMeta('internal.metrics.shuffle.read.remoteBytesReadToDisk', 'size', 'bytes'),
    MetricMeta('internal.metrics.shuffle.write.bytesWritten', 'size', 'bytes'),
    MetricMeta('internal.metrics.shuffle.write.recordsWritten', 'count', 'records'),
    MetricMeta('internal.metrics.shuffle.write.writeTime', 'time', 'ms'),
    MetricMeta('join output rows', 'count', 'rows'),
    MetricMeta('join time', 'time', 'ms'),
    MetricMeta('local blocks read', 'count', 'blocks'),
    MetricMeta('local bytes read', 'size', 'bytes'),
    MetricMeta('number of input columnar batches', 'count', 'batches'),
    MetricMeta('number of input batches', 'count', 'batches'),
    MetricMeta('number of input rows', 'count', 'rows'),
    MetricMeta('number of output columnar batches', 'count', 'batches'),
    MetricMeta('number of output rows', 'count', 'rows'),
    MetricMeta('peak device memory', 'size', 'bytes'),
    MetricMeta('peak memory', 'size', 'bytes'),
    MetricMeta('records read', 'count', 'records'),
    MetricMeta('remote blocks read', 'count', 'blocks'),
    MetricMeta('remote bytes read', 'size', 'bytes'),
    MetricMeta('scan time', 'time', 'ms'),
    MetricMeta('shuffle bytes written', 'size', 'bytes'),
    MetricMeta('shuffle records written', 'count', 'records'),
    MetricMeta('shuffle write time', 'time', 'ns'),
    MetricMeta('spill size', 'size', 'bytes'),
    MetricMeta('sort time', 'time', 'ms'),
    MetricMeta('time in aggregation build', 'time', 'ms'),
    MetricMeta('time in batch concat', 'time', 'ms'),
    MetricMeta('time in compute agg', 'time', 'ms'),
    MetricMeta('total time', 'time', 'ns'),
    MetricMeta('write time', 'time', 'ms'),
    MetricMeta('DirectPoolMemory', 'size', 'bytes'),
    MetricMeta('Disk Bytes Spilled', 'size', 'bytes'),
    MetricMeta('Executor CPU Time', 'time', 'ns'),
    MetricMeta('Executor Deserialize CPU Time', 'time', 'ns'),
    MetricMeta('Executor Deserialize Time', 'time', 'ms'),
    MetricMeta('Executor Run Time', 'time', 'ms'),
    MetricMeta('Input Metrics Bytes Read', 'size', 'bytes'),
    MetricMeta('Input Metrics Records Read', 'count', 'records'),
    # next two without structure prefix
    MetricMeta('Bytes Read', 'size', 'bytes'),
    MetricMeta('Records Read', 'count', 'records'),
    MetricMeta('JVM GC Time', 'time', 'ms'),
    MetricMeta('JVMHeapMemory', 'size', 'bytes'),
    MetricMeta('JVMOffHeapMemory', 'size', 'bytes'),
    MetricMeta('MajorGCCount', 'count', 'collections'),
    MetricMeta('MajorGCTime', 'time', 'ms'),
    MetricMeta('MappedPoolMemory', 'size', 'bytes'),
    MetricMeta('Memory Bytes Spilled', 'size', 'bytes'),
    MetricMeta('MinorGCCount', 'count', 'collections'),
    MetricMeta('MinorGCTime', 'time', 'ms'),
    MetricMeta('OffHeapExecutionMemory', 'size', 'bytes'),
    MetricMeta('OffHeapStorageMemory', 'size', 'bytes'),
    MetricMeta('OffHeapUnifiedMemory', 'size', 'bytes'),
    MetricMeta('OnHeapExecutionMemory', 'size', 'bytes'),
    MetricMeta('OnHeapStorageMemory', 'size', 'bytes'),
    MetricMeta('OnHeapUnifiedMemory', 'size', 'bytes'),
    MetricMeta('Output Metrics Bytes Written', 'size', 'bytes'),
    MetricMeta('Output Metrics Records Written', 'count', 'records'),
    # next two without structure prefix
    MetricMeta('Bytes Written', 'size', 'bytes'),
    MetricMeta('Records Written', 'count', 'records'),
    MetricMeta('Peak Execution Memory', 'size', 'bytes'),
    MetricMeta('ProcessTreeJVMRSSMemory', 'size', 'bytes'),
    MetricMeta('ProcessTreeJVMVMemory', 'size', 'bytes'),
    MetricMeta('ProcessTreeOtherRSSMemory', 'size', 'bytes'),
    MetricMeta('ProcessTreeOtherVMemory', 'size', 'bytes'),
    MetricMeta('ProcessTreePythonRSSMemory', 'size', 'bytes'),
    MetricMeta('ProcessTreePythonVMemory', 'size', 'bytes'),
    MetricMeta('Result Serialization Time', 'time', 'ms'),
    MetricMeta('Result Size', 'size', 'bytes'),
    MetricMeta('Shuffle Read Metrics Fetch Wait Time', 'time', 'ms'),
    MetricMeta('Shuffle Read Metrics Local Blocks Fetched', 'count', 'blocks'),
    MetricMeta('Shuffle Read Metrics Local Bytes Read', 'size', 'bytes'),
    MetricMeta('Shuffle Read Metrics Local Bytes', 'size', 'bytes'),
    MetricMeta('Shuffle Read Metrics Remote Blocks Fetched', 'count', 'blocks'),
    MetricMeta('Shuffle Read Metrics Remote Bytes Read', 'size', 'bytes'),
    MetricMeta('Shuffle Read Metrics Remote Bytes', 'size', 'bytes'),
    MetricMeta('Shuffle Read Metrics Remote Bytes Read To Disk', 'size', 'bytes'),
    MetricMeta('Shuffle Read Metrics Remote Bytes To Disk', 'size', 'bytes'),

    MetricMeta('Shuffle Read Metrics Total Records Read', 'count', 'records'),
    MetricMeta('Shuffle Read Metrics Total Records', 'count', 'records'),
    # next six without structure prefix
    MetricMeta('Fetch Wait Time', 'time', 'ms'),
    MetricMeta('Local Blocks Fetched', 'count', 'blocks'),
    MetricMeta('Local Bytes Read', 'size', 'bytes'),
    MetricMeta('Remote Blocks Fetched', 'count', 'blocks'),
    MetricMeta('Remote Bytes Read', 'size', 'bytes'),
    MetricMeta('Remote Bytes Read To Disk', 'size', 'bytes'),
    MetricMeta('Total Records Read', 'count', 'records'),
    MetricMeta('Shuffle Write Metrics Shuffle Bytes Written', 'size', 'bytes'),
    MetricMeta('Shuffle Write Metrics Shuffle Records Written', 'count', 'records'),
    MetricMeta('Shuffle Write Metrics Shuffle Write Time', 'time', 'ms'),
    MetricMeta('Shuffle Write Metrics Bytes Written', 'size', 'bytes'),
    MetricMeta('Shuffle Write Metrics Records Written', 'count', 'records'),
    MetricMeta('Shuffle Write Metrics Write Time', 'time', 'ms'),
    # next three without structure prefix
    MetricMeta('Shuffle Bytes Written', 'size', 'bytes'),
    MetricMeta('Shuffle Records Written', 'count', 'records'),
    MetricMeta('Shuffle Write Time', 'time', 'ms'), 
]

import altair as alt

def stage_and_task_charts(task_metrics_df, noun="Time"):
    
    selection = alt.selection_multi(name="SelectorName", fields=['Stage ID'], empty='none')
    stage_metrics_df = task_metrics_df.groupby(['Stage ID', 'Metric Name']).sum()
    
    stages = alt.Chart(
        stage_metrics_df.reset_index()
    ).mark_bar().encode(
        x='Stage ID:N',
        y=alt.Y('sum(Metric Value):Q', title=noun),
        color='Metric Name:N',
        tooltip=['Metric Name', 'Metric Value', 'Stage ID']
    ).add_selection(selection).interactive()
    
    tasks = alt.Chart(
        task_metrics_df.reset_index()
    ).mark_bar().encode(
        x='Task ID:N',
        y=alt.Y('Metric Value:Q', title=noun),
        color='Metric Name:N',
        tooltip=['Metric Name', 'Metric Value', 'Task ID']
    ).transform_filter(
        selection
    ).interactive()

    return alt.vconcat(stages, tasks)


def job_and_plan_charts(plan_metrics_df, noun="Time"):
    
    selection = alt.selection_multi(name="SelectorName", fields=['Job ID'], empty='none')
    job_metrics_df = plan_metrics_df.groupby(['Job ID', 'Metric Name']).sum()
    
    jobs = alt.Chart(
        job_metrics_df.reset_index()
    ).mark_bar().encode(
        x='Job ID:N',
        y=alt.Y('sum(Metric Value):Q', title=noun),
        color='Metric Name:N',
        tooltip=['Metric Name', 'Metric Value', 'Job ID']
    ).add_selection(selection).interactive()
    
    nodes = alt.Chart(
        plan_metrics_df.reset_index()
    ).mark_bar().encode(
        x='plan_node:N',
        y=alt.Y('sum(Metric Value):Q', title=noun),
        color='Metric Name:N',
        tooltip=['Metric Name', 'Metric Value', 'simpleString']
    ).transform_filter(
        selection
    ).interactive()

    return alt.vconcat(jobs, nodes)



def layered_stage_and_task_charts(task_layers, noun="Time"):
    
    selection = alt.selection_multi(name="selector_SelectorName", fields=['Stage ID'], empty='none')
    sdfs = [tdf.groupby(['Stage ID', 'Metric Name']).sum() for tdf in task_layers]
    
    stages = alt.layer(*[alt.Chart(
        sdf.reset_index()
    ).mark_bar().encode(
        x='Stage ID:N',
        y=alt.Y('sum(Metric Value):Q', title=noun),
        color='Metric Name:N',
        tooltip=['Metric Name', 'Metric Value', 'Task ID']
    ) for sdf in sdfs]).add_selection(selection).interactive()
    
    tasks = alt.layer(*[alt.Chart(
        tdf.reset_index()
    ).mark_bar().encode(
        x='Task ID:N',
        y=alt.Y('sum(Metric Value):Q', title=noun),
        color='Metric Name:N',
        tooltip=['Metric Name', 'Metric Value', 'Task ID']
    ).transform_filter(
        selection
    ) for tdf in task_layers]).interactive()

    return alt.vconcat(stages, tasks)

def melt(df, id_vars=None, value_vars=None, var_name='variable', value_name='value'):
    if id_vars is None:
        id_vars = []
    
    if value_vars is None:
        value_vars = [c for c in df.columns if c not in id_vars]
    
    return df.withColumn(
        "value_tuple",
        F.explode(
            F.array(
                *[
                    F.struct(
                        F.lit(vv).alias(var_name), 
                        F.col("`%s`" % vv).alias(value_name)
                    ) 
                    for vv in value_vars
                ]
            )
        )
    ).select(*(id_vars + [F.col("value_tuple")[cn].alias(cn) for cn in [var_name, value_name]]))

def meltconfig(raw_df, event):
    if event is not None:
        if isinstance(event, list):
            df = raw_df.where(F.col("Event").isin(event))
        else:
            df = raw_df.where(F.col("Event") == event)
    else:
        df = raw_df
            
    def helper(df, field):
        return melt(df.select(field).dropna().select("%s.*" % field))

    return helper(df, "Properties").union(helper(df, "System Properties")).union(helper(df, "Hadoop Properties")).distinct()
    

def safe_write(df, table, db, **kwargs):
    """ safe_write ensures that the table has all of the necessary columns (or doesn't exist) before writing """
    
    cursor = db.execute("select count(name) from sqlite_master where type='table' and name=?", (table,))
    if cursor.fetchone()[0] == 1:
        # the table already exists
        df_cols = set(df.columns)
        table_cols = set([t[0] for t in db.execute("select * from [%s] limit 1" % table).description])
        
        if 'debug_me' in options and options['debug_me']:
            print("df_cols are %r" % df_cols)
            print("table_cols are %r" % table_cols)
            print("difference is %r" % (df_cols - table_cols))

        for col in df_cols - table_cols:
            db.execute("alter table [%s] add column [%s]" % (table, col))
    df.to_sql(table, db, if_exists='append', **kwargs)


def plan_metrics_rollup(df):
    return df.groupBy(
        ["plan_node", "accumulatorId", "Task ID"]
    ).agg(
        F.sum("Metric Value").alias("Metric Value"),
        F.min("nodeName").alias("nodeName"),
        F.min("simpleString").alias("simpleString"), 
        F.min("metricType").alias("metricType"),
        F.min("name").alias("Metric Name"),
        F.min("Stage ID").alias("Stage ID"),
        F.min("Job ID").alias("Job ID"),
        F.min("Application ID").alias("Application ID"),
        F.min("Application Name").alias("Application Name")
    ).withColumn(
        "Metric Value", 
        F.when(
            F.col("metricType") == "nsTiming", 
            F.col("Metric Value") / 1000000
        ).otherwise(F.col("Metric Value"))
    ).withColumn(
        "metricType", 
        F.when(
            F.col("metricType") == "nsTiming", 
            F.lit("timing")
        ).otherwise(F.col("metricType"))
    )

def metric_names_for(plan_metrics_df):
    metric_set = set([
        'GPU decode time',
        'GPU time',
        'buffer time',
        'collect batch time',
        'concat batch time', 
        'data size',
        'duration',
        'fetch wait time',
        'local blocks read', 
        'local bytes read',
        'number of input columnar batches',
        'number of input rows',
        'number of output columnar batches',
        'number of output rows',
        'peak device memory',
        'records read',
        'scan time',
        'shuffle bytes written',
        'shuffle records written',
        'shuffle write time',
        'sort time',
        'time in compute agg',
        'total time',
        'write time'
    ]) | set([
        r[0] for r 
        in plan_metrics_df.select(
            "Metric Name"
        ).distinct().collect()
    ])

    return list(sorted(metric_set))
