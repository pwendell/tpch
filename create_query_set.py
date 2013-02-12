import random

TIMES_PER_QUERY=500
NUM_PERMUTATIONS=15

random.seed(12345)

def get_file(filename):
  return "".join(open(filename).readlines())

queries_to_add = ["denorm_tpch/q3_shipping_priority_confirmed.hive",
                  "denorm_tpch/q4_order_priority_confirmed.hive",
                  "denorm_tpch/q6_forecast_revenue_change_confirmed.hive",
#                  "denorm_tpch/q2_minimum_cost_supplier_confirmed.hive",
                  "denorm_tpch/q12_shipping_confirmed.hive"]

denorm = get_file("make_denorm_cached.hql")

for x in range(NUM_PERMUTATIONS):
  f = open("workloads/tpch_workload_%s" % (x + 1), 'w')
  f.write(denorm)
  for k in range(TIMES_PER_QUERY):
    random.shuffle(queries_to_add)
    for q in queries_to_add:
      query = get_file(q)
      f.write(query)
  f.close()
