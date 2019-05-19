package multiJoin;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TPCH {
	public static void main(String[] args) throws Exception{

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		if(args.length != 4){
			System.out.println("the use is   input  output parallelism field");
			return;
		}
		String input = args[0];
		String output = args[1];
		int parallelism = Integer.parseInt(args[2]);
		int field = Integer.parseInt(args[3]);

		//设置并行度
		env.setParallelism(parallelism);

		env.setParallelism(2);
		//DataSet<Order> orders = getOrderDataSet(env,input);
		DataSet<Partsupp> partsuppDataSet = getPartsuppDataSet(env,input);
		DataSet<Partsupp> afterRange=partsuppDataSet.partitionByRange(3,0,2);
		afterRange.writeAsText(output,FileSystem.WriteMode.OVERWRITE);
		env.execute();
		//DataSet<Supplier> suppliers = getSupplierDataSet(env);

//        DataSet<Lineitem> lineitems = getLineitemDataSet(env);
//        DataSet<Order> orders = getOrderDataSet(env);
//        DataSet<Customer> customers = getCustomerDataSet(env);
//        DataSet<Nation> nations1 = getNationDataSet(env);
//        DataSet<Nation> nations2 = getNationDataSet(env);
//
//        lineitems = lineitems.filter(
//                new FilterFunction<Lineitem>() {
//                    private final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
//                    private final Date data1 = format.parse("1995-01-01");
//                    private final Date data2 = format.parse("1996-12-31");
//
//                    @Override
//                    public boolean filter(Lineitem lineitem) throws Exception {
//                        return format.parse(lineitem.getShipdate()).after(data1) && format.parse(lineitem.getShipdate()).before(data2);
//                    }
//                });
		//suppliers.print();
//        lineitems.writeAsCsv("/home/wsy/IdeaProjects/FlinkReviseExample/result/TPCH_Q7/filter/lineitems_f.csv","\n","|", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

//        //nationWithSupplier: nationkey name || suppkey nationkey   共同的连接属性用第一个表的  连接属性：nationkey
//        DataSet<Tuple4<Long, String, Long, Long>> nationWithSupplier = nations1.join(suppliers)
//                .where(0).equalTo(1).projectFirst(0, 1).projectSecond(0, 1);
//
//        //ntspWithLineitem: nationkey name | suppkey nationkey || orderkey suppkey shipdate  连接属性:suppkey
//        DataSet<Tuple7<Long, String, Long, Long, Long, Long, String>> ntspWithLineitem = nationWithSupplier.join(lineitems)
//                .where(2).equalTo(1).projectFirst(0, 1, 2, 3).projectSecond(0, 1, 2);
//
//        //ntspliWithOrder: nationkey name | suppkey nationkey |  orderkey suppkey shipdate || orderkey custkey 连接属性：orderkey
//        DataSet<Tuple9<Long, String, Long, Long, Long, Long, String, Long, Long>> ntspliWithOrder = ntspWithLineitem.join(orders)
//                .where(4).equalTo(0).projectFirst(0, 1, 2, 3, 4, 5, 6).projectSecond(0, 1);
//
//        //ntspliodWithCustomer: nationkey name | suppkey nationkey | orderkey suppkey shipdate | orderkey custkey || custkey nationkey 连接属性：custkey
//        DataSet<Tuple11<Long, String, Long, Long, Long, Long, String, Long, Long, Long, Long>> ntspliodWithCustomer = ntspliWithOrder.join(customers)
//                .where(8).equalTo(0).projectFirst(0, 1, 2, 3, 4, 5, 6, 7 ,8).projectSecond(0, 1);
//
//        //nationkey name suppkey orderkey shipdate custkey nationkey || nationkey name --> name | name 连接属性：nationkey
////		DataSet<Tuple2<String, String>> finalJnResult = ntspliodWithCustomer.join(nations2)
////			.where(6).equalTo(0).projectFirst(1).projectSecond(8);
//
//        //finalJnResult2: nationkey name | suppkey nationkey | orderkey suppkey shipdate | orderkey custkey | custkey nationkey || nationkey name --> name | name 连接属性：nationkey
//        DataSet<Tuple13<Long, String, Long, Long, Long, Long, String, Long, Long, Long, Long, Long, String>> jnResult = ntspliodWithCustomer.join(nations2)
//                .where(10).equalTo(0).projectFirst(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).projectSecond(0, 1);
////        DataSet<Tuple2<String, String>> finalJnResult2 = jnResult.project(1,7);
//
//        jnResult.writeAsCsv("/home/hadoop/wsy/result/TPCH_Q7/joinResult.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		//env.execute("TPCH_Q7");
//        System.out.println(env.getExecutionPlan());

	}

	// *************************************************************************
	//     DATA TYPES
	// *************************************************************************

	/**
	 * Supplier.
	 */
	public static class Supplier extends Tuple2<Long, Long> {

		public Long getSuppkey() {
			return this.f0;
		}

		public Long getNationkey() {
			return this.f1;
		}
	}

	public static class Lineitem extends Tuple3<Long, Long, String> {

		public Long getSuppkey() {
			return this.f0;
		}

		public Long getOrderkey() {
			return this.f1;
		}

		public String getShipdate() {
			return this.f2;
		}

	}

	public static class Order extends Tuple9<Long, Long, String, String, String, String, String, String, String > {

		public Long getOrderkey() {
			return this.f0;
		}

		public Long getCustkey() {
			return this.f1;
		}

		public String getOthers(){
			return this.f2+" "+this.f3+" "+this.f4+" "+this.f5+" "+this.f6+" "+this.f7+" "+this.f8;
		}
	}

	public static class Customer extends Tuple2<Long, Long> {

		public Long getCustkey() {
			return this.f0;
		}

		public Long getNationKey() {
			return this.f1;
		}
	}

	public static class Nation extends Tuple2<Long, String> {

		public Long getNationkey() {
			return this.f0;
		}

		public String getName() {
			return this.f1;
		}
	}

	public static class Partsupp extends Tuple5<Long, Long, String, String, String> {
		public Long getPartkey() {
			return this.f0;
		}

		public Long getSuppkey() {
			return this.f1;
		}

		public String getOthers(){
			return this.f2+" "+this.f3+" "+this.f4;
		}
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static DataSet<Supplier> getSupplierDataSet(ExecutionEnvironment env) {
		return env.readCsvFile("D:\\批流融合大数据\\工具\\tables\\supplier.tbl")
			.fieldDelimiter("|")
			.includeFields("1001000")
			.tupleType(Supplier.class);
	}

	private static DataSet<Lineitem> getLineitemDataSet(ExecutionEnvironment env) {
		return env.readCsvFile("hdfs://master:9000/wsy/source/2G/lineitem.csv")
			.fieldDelimiter("|")
			.includeFields("1010000000100000")
			.tupleType(Lineitem.class);
	}

	private static DataSet<Partsupp> getPartsuppDataSet(ExecutionEnvironment env, String input) {
		return env.readCsvFile(input)
				.fieldDelimiter("|")
				.includeFields("11111")
				.tupleType(Partsupp.class);
	}

	private static DataSet<Order> getOrderDataSet(ExecutionEnvironment env, String input) {
		return env.readCsvFile(input)
			.fieldDelimiter("|")
			.includeFields("111111111")
			.tupleType(Order.class);
	}

	private static DataSet<Customer> getCustomerDataSet(ExecutionEnvironment env) {
		return env.readCsvFile("hdfs://master:9000/wsy/source/2G/customer.csv")
			.fieldDelimiter("|")
			.includeFields("10010000")
			.tupleType(Customer.class);
	}

	private static DataSet<Nation> getNationDataSet(ExecutionEnvironment env) {
		return env.readCsvFile("hdfs://master:9000/wsy/source/2G/nation.csv")
			.fieldDelimiter("|")
			.includeFields("1100")
			.tupleType(Nation.class);
	}

}
