
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.locks.Lock;


public class PopulationQuery {
	// next four constants are relevant to parsing
    public static final int PARTS = 4;
    public static final int GRID_CUTOFF = 400;
    public static final int SEQUENTIAL_CUTOFF = 20;
	public static final int TOKENS_PER_LINE  = 7;
	public static final int POPULATION_INDEX = 4; // zero-based indices
	public static final int LATITUDE_INDEX   = 5;
	public static final int LONGITUDE_INDEX  = 6;
    public static int[][] grid;
	// public static final Lock lock = new Lock();


	// parse the input file into a large array held in a CensusData object
	public static CensusData parse(String filename) {
		CensusData result = new CensusData();

        try {
            BufferedReader fileIn = new BufferedReader(new FileReader(filename));

            // Skip the first line of the file
            // After that each line has 7 comma-separated numbers (see constants above)
            // We want to skip the first 4, the 5th is the population (an int)
            // and the 6th and 7th are latitude and longitude (floats)
            // If the population is 0, then the line has latitude and longitude of +.,-.
            // which cannot be parsed as floats, so that's a special case
            //   (we could fix this, but noisy data is a fact of life, more fun
            //    to process the real data as provided by the government)

            String oneLine = fileIn.readLine(); // skip the first line

            // read each subsequent line and add relevant data to a big array
            while ((oneLine = fileIn.readLine()) != null) {
                String[] tokens = oneLine.split(",");
                if(tokens.length != TOKENS_PER_LINE)
                	throw new NumberFormatException();
                int population = Integer.parseInt(tokens[POPULATION_INDEX]);
                if(population != 0)
                	result.add(population,
                			   Float.parseFloat(tokens[LATITUDE_INDEX]),
                		       Float.parseFloat(tokens[LONGITUDE_INDEX]));
            }

            fileIn.close();
        } catch(IOException ioe) {
            System.err.println("Error opening/reading/writing input or output file.");
            System.exit(1);
        } catch(NumberFormatException nfe) {
            System.err.println(nfe.toString());
            System.err.println("Error in file format");
            System.exit(1);
        }
        return result;
	}

	// argument 1: file name for input data: pass this to parse
	// argument 2: number of x-dimension buckets
	// argument 3: number of y-dimension buckets
    // argument 4: -v1, -v2, -v3, -v4, or -v5
	public static void main(String[] args) {
		// FOR YOU
        int x = Integer.parseInt(args[1]);
        int y = Integer.parseInt(args[2]);
        PopulationQuery testing = new PopulationQuery();
        CensusData test = testing.parse(args[0]);
        String version = args[3];
        Rectangle corners = new Rectangle(0, 0, 0, 0);
        int total = test.totalpop;

        /////////////////////////////////// PART 1 SEQUENTIAL //////////////////////////////////////////////////////////////////////////////////

        if (version.equals("v1")) {
            corners = new Rectangle(test.data[0].longitude, test.data[0].longitude, test.data[0].latitude, test.data[0].latitude);
            for (int i = 0; i < test.data.length; i++) {
                if (test.data[i] != null) {
                    Rectangle temp = new Rectangle(test.data[i].longitude, test.data[i].longitude, test.data[i].latitude, test.data[i].latitude);
                    corners = temp.encompass(corners);
                }
            }
            System.out.println(corners.toString());
        }
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        ////////////////////////////////////// Part 2 Simple and Parallel //////////////////////////////////////////////////////////////////////

        if (version.equals("v2")) {
            corners = formrectangle(test, 0, test.data.length);
            System.out.println(corners.toString());
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        ////////////////////////////////////// Part 3 Complex and Sequential ///////////////////////////////////////////////////////////////////

        grid = new int[y][x];

        if (version.equals("v3")) {
            corners = new Rectangle(test.data[0].longitude, test.data[0].longitude, test.data[0].latitude, test.data[0].latitude);
            for (int i = 0; i < test.data.length; i++) {
                if (test.data[i] != null) {
                    Rectangle temp = new Rectangle(test.data[i].longitude, test.data[i].longitude, test.data[i].latitude, test.data[i].latitude);
                    corners = temp.encompass(corners);
                }
            }
            System.out.println(corners.toString());

            for (int i = 0; i < test.data.length; i++) {
                if (test.data[i] != null) {
                    int temp1 = (int)(((test.data[i].longitude - corners.left) / (corners.right - corners.left)) * x);
                    int temp2 = (int)(((test.data[i].latitude - corners.bottom) / (corners.top - corners.bottom)) * y);
                    if (temp1 == x) {
                        temp1--;
                    }
                    if (temp2 == y) {
                        temp2--;
                    }
                    grid[temp2][temp1] += test.data[i].population;
                }
            }

            for (int j = (y-1); j >= 0; j--) {
                for (int i = 0; i < x; i++) {
                    int orig = grid[j][i];
                    if (i != 0) {
                        grid[j][i] = orig + grid[j][i-1];
                    }
                    if  (j != (y-1)) {
                        grid[j][i] += grid[j+1][i];
                    }
                    if ((j != (y-1)) && (i != 0)) {
                        grid[j][i] -= grid[j+1][i-1];
                    }
                }
            }
        }
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        ////////////////////////////////////// Part 4 Complex and Parallel /////////////////////////////////////////////////////////////////////

        if (version.equals("v4")) {
            corners = formrectangle(test, 0, test.data.length);
            System.out.println(corners.toString());

            grid = formgrid(test, corners, x, y, 0, test.data.length);

            for (int j = (y-1); j >= 0; j--) {
                for (int i = 0; i < x; i++) {
                    int orig = grid[j][i];
                    if (i != 0) {
                        grid[j][i] = orig + grid[j][i-1];
                    }
                    if  (j != (y-1)) {
                        grid[j][i] += grid[j+1][i];
                    }
                    if ((j != (y-1)) && (i != 0)) {
                        grid[j][i] -= grid[j+1][i-1];
                    }
                }
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        ////////////////////////////////////// Part 5 Complex and Locks ////////////////////////////////////////////////////////////////////////

        if (version.equals("v5")) {
            corners = formrectangle(test, 0, test.data.length);
            System.out.println(corners.toString());

            final int SIZE = test.data.length;
            final int PARTS_SZ = SIZE/PARTS;
            Object[][] locks = new Object[y][x];
            for (int i = 0; i<x; i++) {
                for (int j = 0; j<y; j++) {
                    locks[j][i] = new Object();
                }
            }

            final Rectangle cornermain = new Rectangle(corners.left, corners.right, corners.top, corners.bottom);
            Thread[] threadList = new Thread[PARTS];
            for (int i = 0; i<PARTS; ++i) {
                final int tid = i;
                final int xdim = x;
                final int ydim = y;
                threadList[tid] = new Thread(() -> {
                    for (int j = tid*PARTS_SZ; j < (tid+1)*PARTS_SZ; ++j) {
                        if (test.data[j] != null) {
                            int temp1 = (int)(((test.data[j].longitude - cornermain.left) / (cornermain.right - cornermain.left)) * xdim);
                            int temp2 = (int)(((test.data[j].latitude - cornermain.bottom) / (cornermain.top - cornermain.bottom)) * ydim);
                            if (temp1 == x) {
                                temp1--;
                            }
                            if (temp2 == y) {
                                temp2--;
                            }
                            synchronized (locks[temp2][temp1]) {
                                grid[temp2][temp1] += test.data[j].population;
                            }
                        }
                    }
                });
                threadList[tid].start();
            }

            for (int i = 0; i < PARTS; ++i) {
                try {
                    threadList[i].join();
                } catch (Exception e) {}
            }

            for (int j = (y-1); j >= 0; j--) {
                for (int i = 0; i < x; i++) {
                    int orig = grid[j][i];
                    if (i != 0) {
                        grid[j][i] = orig + grid[j][i-1];
                    }
                    if  (j != (y-1)) {
                        grid[j][i] += grid[j+1][i];
                    }
                    if ((j != (y-1)) && (i != 0)) {
                        grid[j][i] -= grid[j+1][i-1];
                    }
                }
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        while (true) {
            System.out.println("Enter the 4 numbers describing the grid: ");
            Scanner s = new Scanner(System.in);
            String query = s.nextLine();
            String[] temp = query.split(" ");
            if (temp.length != 4) {
                System.out.println("Exiting the program...");
                break;
            }
            Float[] coordinates = new Float[4];
            for (int i = 0; i < coordinates.length; i++) {
                coordinates[i] = Float.parseFloat(temp[i]);
            }
            checkargvalid(coordinates, x, y);
            float west = (((coordinates[0] - 1) / (x)) * (corners.right - corners.left) + corners.left);
            float south = (((coordinates[1] - 1) / (y)) * (corners.top - corners.bottom) + corners.bottom);
            float east = (((coordinates[2]) / (x)) * (corners.right - corners.left) + corners.left);
            float north = (((coordinates[3]) / (y)) * (corners.top - corners.bottom) + corners.bottom);
            int population = 0;

        /////////////////////////////////// PART 1 SEQUENTIAL //////////////////////////////////////////////////////////////////////////////////

            if (version.equals("v1")) {
                for (int i = 0; i < test.data.length; i++) {
                    if (test.data[i] != null) {
                        // System.out.println(test.data[i].latitude);
                        if (test.data[i].longitude >= west && test.data[i].longitude <= east && test.data[i].latitude >= south && test.data[i].latitude <= north) {
                            population += test.data[i].population;
                        }
                    }
                }
            }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        ////////////////////////////////////// Part 2 Simple and Parallel //////////////////////////////////////////////////////////////////////

            if (version.equals("v2")) {
                population = processquery(test, 0, test.data.length, west, east, south, north);
            }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        ////////////////////////////////////// Part 3 Complex and Sequential ///////////////////////////////////////////////////////////////////

            if (version.equals("v3") || version.equals("v4") || version.equals("v5")) {
                float lefty = coordinates[0];
                float bot = coordinates[1];
                float righty = coordinates[2];
                float topp = coordinates[3];
                int left = (int)lefty;
                int bottom = (int)bot;
                int right = (int)righty;
                int top = (int)topp;
                population += grid[bottom-1][right-1];
                if (top != y) {
                    population -= grid[top][right-1];
                }
                if ((left - 1) != 0) {
                    population -= grid[bottom-1][left-2];
                }
                if ((top != y) && ((left - 1) != 0)) {
                    population += grid[top][left-2];
                }
            }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            System.out.println("Population for Query: " + population);
            System.out.println("Percentage of U.S population for Query: " + (((float)population)/((float)total) * 100.0));
        }
	}

    public static int processquery(CensusData test, int lo, int hi, float west, float east, float south, float north) {
        if (hi - lo < SEQUENTIAL_CUTOFF) {
            int population = 0;
            for (int i = lo; i < hi; i++) {
                if (test.data[i] != null) {
                    // System.out.println(test.data[i].latitude);
                    if (test.data[i].longitude >= west && test.data[i].longitude <= east && test.data[i].latitude >= south && test.data[i].latitude <= north) {
                        population += test.data[i].population;
                    }
                }
            }
            return population;
        } else {
            ForkJoinTask<Integer> left =
                ForkJoinTask.adapt(() -> processquery(test, lo, (hi+lo)/2, west, east, south, north)).fork();
            int rightAns = processquery(test, (hi+lo)/2, hi, west, east, south, north);
            int leftAns = left.join();
            return leftAns + rightAns;
        }
    }

    public static int[][] formgrid(CensusData test, Rectangle corners, int x, int y, int lo, int hi) {
        if (hi - lo < SEQUENTIAL_CUTOFF) {
            int grid[][] = new int[y][x];
            for (int i = lo; i < hi; i++) {
                if (test.data[i] != null) {
                    int temp1 = (int)(((test.data[i].longitude - corners.left) / (corners.right - corners.left)) * x);
                    int temp2 = (int)(((test.data[i].latitude - corners.bottom) / (corners.top - corners.bottom)) * y);
                    if (temp1 == x) {
                        temp1--;
                    }
                    if (temp2 == y) {
                        temp2--;
                    }
                    grid[temp2][temp1] += test.data[i].population;
                }
            }
            return grid;
        } else {
            ForkJoinTask<int[][]> left =
                ForkJoinTask.adapt(() -> formgrid(test, corners, x, y, lo, (hi+lo)/2)).fork();
            int[][] rightAns = formgrid(test, corners, x, y, (hi+lo)/2, hi);
            int[][] leftAns = left.join();
            int[][] combined = sumgrid(0, 0, x, y, leftAns, rightAns);
            return combined;
        }
    }

    public static int[][] sumgrid(int lox, int loy, int hix, int hiy, int[][] leftAns, int[][] rightAns) {
        if ((hix-loy)*(hiy-loy) < GRID_CUTOFF) {
            for (int i = lox; i < hix; i++) {
                for (int j = loy; j < hiy; j++) {
                    leftAns[j][i] += rightAns[j][i];
                }
            }
            return leftAns;
        } else {
            ForkJoinTask<int[][]> sw =
                ForkJoinTask.adapt(() -> sumgrid(lox, loy, (hix+lox)/2, (hiy+loy)/2, leftAns, rightAns)).fork();
            ForkJoinTask<int[][]> se =
                ForkJoinTask.adapt(() -> sumgrid((lox+hix)/2, loy, hix, (hiy+loy)/2, leftAns, rightAns)).fork();
            ForkJoinTask<int[][]> nw =
                ForkJoinTask.adapt(() -> sumgrid(lox, (loy+hiy)/2, (hix+lox)/2, hiy, leftAns, rightAns)).fork();
            int[][] ne = sumgrid((lox+hix)/2, (loy+hiy)/2, hix, hiy, leftAns, rightAns);
            int[][] nwans = nw.join();
            int[][] seans = se.join();
            int[][] swans = sw.join();
            return leftAns;
        }
    }


    public static Rectangle formrectangle(CensusData test, int lo, int hi) {
        if (hi - lo < SEQUENTIAL_CUTOFF) {
            Rectangle corners = new Rectangle(test.data[0].longitude, test.data[0].longitude, test.data[0].latitude, test.data[0].latitude);
            for (int i = lo; i < hi; i++) {
                if (test.data[i] != null) {
                    Rectangle temp = new Rectangle(test.data[i].longitude, test.data[i].longitude, test.data[i].latitude, test.data[i].latitude);
                    corners = temp.encompass(corners);
                }
            }
            return corners;
        } else {
            ForkJoinTask<Rectangle> left =
                ForkJoinTask.adapt(() -> formrectangle(test, lo, (hi+lo)/2)).fork();
            Rectangle rightAns = formrectangle(test, (hi+lo)/2, hi);
            Rectangle leftAns = left.join();
            return leftAns.encompass(rightAns);
        }
    }

    public static void checkargvalid(Float[] coordinates, int x, int y) {
        if (coordinates[0] < 1 || coordinates[0] > x) {
            System.err.println("Error in arguments provided");
            System.exit(1);
        }
        if (coordinates[1] < 1 || coordinates[1] > y) {
            System.err.println("Error in arguments provided");
            System.exit(1);
        }
        if (coordinates[2] < coordinates[0] || coordinates[2] > x) {
            System.err.println("Error in arguments provided");
            System.exit(1);
        }
        if (coordinates[3] < coordinates[1] || coordinates[3] > x) {
            System.err.println("Error in arguments provided");
            System.exit(1);
        }
    }
}
