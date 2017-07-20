package tiger.spark.example;

public class TEST {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int reqSum = 0;
		String l = "aaa_5%bbb_6%cccc_7%dddd_9";
		String[] res = l.split("%");
		for(String s : res){
			
			System.out.println(s);
			
		if(s != "")
				
				reqSum += Integer.parseInt(s.split("_")[1]);
			
			System.out.println(reqSum);
		}

	}

}
