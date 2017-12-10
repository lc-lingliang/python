# Imports the Google Cloud client library
from google.cloud import bigquery
from datetime import date

# Instantiates a client
client = bigquery.Client(project='localcover-55')

# QUERY = (
#     'SELECT PolicyId, SalesWeek, premium_exGst, productClass, productBrand,'
#     'productModel, WtyProductCode, YrPurchased, mthPurchased '
#     'FROM `ling.sales` '
#     'WHERE wtyTerm = 12' )
QUERY = (
    'SELECT s.PolicyId as PolicyId, YrPurchased as YrSales, mthPurchased as MthSales, '
    's.premium_exGst as Premium_exGst, c.ClaimId as ClaimId, '
    's.productClass as Class, s.productBrand as Brand, s.productModel as Model, '
    # '((YEAR(ClaimDate) - YrPurchased)*12 + (MONTH(ClaimDate) - mthPurchased)) AS MthClaim, '
    'WtyProductCode, c.claimCostexGST as ClaimCost '
    'FROM `ling.sales` as s '
    'LEFT OUTER JOIN `ling.claims` as c '
    'ON s.PolicyId = c.PolicyId '
    'WHERE s.wtyTerm = 12 '
    'ORDER BY SalesWeek '    
)

TIMEOUT = 10  # in seconds
query_job = client.query(QUERY)  # API request - starts the query

# Waits for the query to finish
iterator = query_job.result(timeout=TIMEOUT)
rows = list(iterator)

assert query_job.state == 'DONE'
print(len(rows))
print(rows[1])    
# The name for the new dataset
#dataset_id = 'lc_api_product'

# Prepares a reference to the new dataset
#dataset_ref = bigquery_client.dataset(dataset_id)
#dataset = bigquery.Dataset(dataset_ref)
#
# Creates the new dataset
#dataset = bigquery_client.create_dataset(dataset)
#
#print('Dataset {} created.'.format(dataset.dataset_id))

##for dataset in bigquery_client.list_datasets():  # API request(s)
##    print('dataset:', dataset.dataset_id)

dataset_id = 'ling'
dataTableSales = 'sales'
dataTableClaims= 'claims'
