#-*- coding:utf-8 -*-

if __name__ == "__main__":
    g = open("/Users/dakimura/go/src/github.com/dakimura/marketstore/contrib/xignitefeeder/test/integ/tests/historical_stocks_20190606.txt", "w")
    with open("/Users/dakimura/go/src/github.com/dakimura/marketstore/contrib/xignitefeeder/test/integ/tests/st.txt", "r") as f:
        for line in f:
            sym = line.split("/")[9]
            g.write(sym +"\n")
    g.close()