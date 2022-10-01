# Outputs a cell.
# Each cell is 28x28 and centered in 20x20 see "Users Guide", https://www.nist.gov/srd/nist-special-database-19
# Run: awk -f (this script) < (database)
# @author Ron.Coleman
# @date 1 Oct 2022
# @see https://www.kaggle.com/datasets/sachinpatel21/az-handwritten-alphabets-in-csv-format?resource=download
BEGIN {
  FS=","
  # 1+28x28: first number is the label.
  EXPECTED_ROW_SIZE = 1+28*28
  
  ALPHAS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  #ROWNO=13871 # start of Bs
  ROWNO=22539 # start of Cs
}
NR == ROWNO {
  n=split($0,cell,",")
  
  if(n != EXPECTED_ROW_SIZE) {
    printf("bad cell size %d\n",ROWNO)
    exit(1)
  }
} 
END {
  print("cell count: " NR)
  # +1 as awk uses 1-based indexes
  label = substr(ALPHAS,cell[1]+1,1)
  print("label: " label)
  
  # Output a cell
  # Pixels start in column 2
  for(i=2; i <= n; i++) {
    if(i != 2 && ((i-2) % 28) == 0)
      printf("\n")
    # 255-9 gives digits between 9-0, rest will be "."
    pix = cell[i]-(255-9)
    if(pix >= 0)
      s = "" pix
    else {
      s="."
    }
    printf("%s",s)
  }
}