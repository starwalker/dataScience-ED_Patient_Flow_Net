import pandas as pd
import pickle
def getData(dsn,sql):

    """
    :param dsn: Data Source Name using trusted connection
    :param sql: SQL query string
    :return: pandas data frame of the results of the query
    """

    import pandas as pd
    import pyodbc
    dsnString ='DSN='+dsn+';Trusted_Connection=yes'
    print(dsnString)
    conn = pyodbc.connect(dsnString)
    data = pd.read_sql_query(sql, conn)
    conn.close()
    return data


# read in data from statmods stored proc
sql = 'SELECT * FROM [StatisticalModels].[dbo].[ED_FLOWv2] ORDER BY 2,1;'
data = getData('statmods', sql)

# save the pickled data
file_name = 'ed_flow_net_data.p'
pickle.dump(data, open(file_name, 'wb'))


# load data
data = pd.read_pickle(file_name)

csv_name = 'ed_flow_net_data.csv'
pd.DataFrame.to_csv(data, csv_name)

#aggregate by count and syum
import numpy as np
grouped = data.groupby(['OriginNode', 'TargetNode'])['TimeDifference'].agg([np.mean, np.count_nonzero])
grouped = grouped.loc[grouped['count_nonzero'] > 50]
csv_name = 'ed_flow_network_agg.csv'
pd.DataFrame.to_csv(grouped , csv_name)