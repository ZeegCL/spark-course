from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalSpentByCustomer")
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amountSpent = float(fields[2])
    return (customerID, amountSpent)


file = "file:///D:/workspace/spark-course/data/customer-orders.csv"
raw = sc.textFile(file)
parsedLines = raw.map(parseLine)
customerAmountsSorted = parsedLines.reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1])
results = customerAmountsSorted.collect()

for result in results:
    customerID = result[0]
    amountSpent = result[1]
    print("Customer ID: {} - Total amount spent: {:.2f}".format(customerID, amountSpent))

