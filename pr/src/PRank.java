// Use Sparse Array to make PageRank

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.*;
import java.util.*;

public class PRank {
    final static double D = 0.85;
    final static int N = 875713;

    public static void main(String[] args) {
        // file path and the time that needs to iterate.
        String dataSource = null;
        int iterTimes = 40;

        if(args.length < 2){
            System.out.println("invalid argument.");
            System.exit( 1 );
        }
        if(args.length>2){
            try{
                iterTimes = Integer.parseInt( args[2] );
            }
            catch (Exception e){
                System.err.println("it is not an integer.");
            }
        }

        // int node_max_size = 0;
        Set<Integer> sameFromDes = new HashSet<>();

        // build the connection between destination and sparse array.
        Map<Integer,Map<Integer,Double>> sparseMatrix = new HashMap<>();
        Map<Integer, Double> R = new HashMap<>();

        File nodesData = new File( args[0] );
        try {
            BufferedReader br = new BufferedReader( new FileReader( nodesData ) );
            String oneLine="";
            int tmp_From = -1; // the tmp id of the node.
            int count = 0;  // count the nodes number.
            int lines = 0; // lines number;
//            int node_max_size = 0; // to initial the bucket for the starting pr.
            oneLine = br.readLine();
            do{
                lines++;
                if( oneLine != null){
                    if(oneLine.charAt( 0 ) != '#') {
//                        System.out.println(oneLine);
                        String[] From_des = oneLine.split( "\\s+" );
                        int from = Integer.parseInt( From_des[0] );
                        int des = Integer.parseInt( From_des[1] );
//                        node_max_size = Math.max( node_max_size, Math.max( from, des ) );
                        // a different FromNodeID, reset the count and size
                        if (tmp_From != from) {
                            count++;
                            // if the set of destinations with same source node is empty, ignore this set.
                            if (!sameFromDes.isEmpty()) {

                                // add the an element in the Sparse Matrix
                                for (Integer tmp_des : sameFromDes) {
                                    Map<Integer,Double> col_val = new HashMap<>();
                                    col_val.put( from, (double) 1/sameFromDes.size() );
                                    sparseMatrix.put( tmp_des, col_val );

                                    /**
                                     * Considering if there is repeat links...
                                     * */
//                                    Map<Integer,Double> col_val = sparseMatrix.get( tmp_des );
//                                    // new row
////                                    System.out.println(tmp_des+" "+sparseMatrix.get( tmp_des ) + " : " + col_val);
//                                    if(col_val == null){
//                                        Map<Integer, Double> tmp_des_pr_map = new HashMap<>();
//                                        tmp_des_pr_map.put( tmp_From, (double) 1 / sameFromDes.size() );
//                                        sparseMatrix.put( tmp_des, tmp_des_pr_map );
//                                    }
//                                    else{
//                                        // new column and value
//                                        if(col_val.get(tmp_From) == null){
//                                            sparseMatrix.get( tmp_des ).put( tmp_From, (double) 1 / sameFromDes.size() );
//                                        }
//                                        else{
//                                            // exiting row and col, means one node go to a side twice.
//                                            double rp = col_val.get(tmp_From);
//                                            sparseMatrix.get( tmp_des ).put( tmp_From, rp + (double) 1 / sameFromDes.size() );
//                                        }
//                                    }

                                    /**
                                     * The code above is Considering if there is repeat links...
                                     * */
                                }
                            }
                            // update the fromNodeID
                            tmp_From = from;
                        }
                        // same site out to another site
                        sameFromDes.add( des );
                    }
                    if(count % 10000 == 0 || lines % 10000 == 0){
                        System.out.println("Log:: Not dead- build " + count + " rows, read " +lines+ "lines.");
                        System.out.println("Log:: record: " + sparseMatrix.size() + " destination sites");
                    }
                    oneLine = br.readLine();
                }
                else{
                    break;
                }
            }while(oneLine != null);
            System.out.println("log::  finish read file");
            // Initialize the PR value R:
            Iterator<Integer> iter = sparseMatrix.keySet().iterator();

            while (iter.hasNext()) {
                R.put( iter.next(), (double) 1/count );
            }
            System.out.println("finish Initialize the PR");
            // begin to calculate the PR:

            int times = 0;
            while(!ifStable( R,0.000001 )){
                times++;
                System.out.println("caculate matrix " + times + " time(s)");
                R = calculatePR( sparseMatrix,R );
            }

            System.out.println("Log:: write the result into the file.");
            writeFilse("output", R);



        } catch (FileNotFoundException e) {
            System.err.println("can't open the file");
        } catch (IOException e) {
            System.err.println("can't read the file");
        }
    }

    private static Map calculatePR(Map<Integer,Map<Integer,Double>> SMatrix, Map<Integer,Double> PRValue){
        Map<Integer, Double> updatedPRV = new HashMap<>();

        Iterator<Integer> row = SMatrix.keySet().iterator();
        while(row.hasNext()){
            Iterator<Integer> col = SMatrix.keySet().iterator();
            int rowNum = row.next();
            while(col.hasNext()){
                int colNum = col.next();
                if(SMatrix.get( rowNum ).get( colNum ) != null && PRValue.get( rowNum)!= null)
                    updatedPRV.put( rowNum, (1-D)/N + D * SMatrix.get( rowNum ).get( colNum ) * PRValue.get( rowNum)) ;
            }
        }
        return updatedPRV;
    }

    private static boolean ifStable(Map<Integer,Double> PRValue , double factor){
        Iterator<Double> PRV = PRValue.values().iterator();
        double distance = 0;

        while(PRV.hasNext()){
            distance += Math.pow( PRV.next(), 2);
        }
        distance = Math.sqrt( distance );
        return distance < factor;
    }

    private static void writeFilse(String path, Map R) throws IOException {
        File output = new File( path );
        if(!output.exists()) output.createNewFile();
        BufferedWriter bw = new BufferedWriter( new FileWriter( output,false ) );
        Iterator<Integer> keys = R.keySet().iterator();
        while (keys.hasNext())
            bw.write( keys.toString() + " : " + R.get( keys ) );
    }

}
