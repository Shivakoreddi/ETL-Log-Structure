import time
##Multi threaded ETL pipelines

def extract():
    print(1)

def transformation():
    print(2)


def load():
    print(3)


def main():
    start = time.time()
    extract()
    transformation()
    load()
    end = time.time()-start
    print("no threads took : {} seconds".format(end))
if __name__=="__main__":
    main()