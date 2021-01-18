package com.ashish.main;

import com.ashish.aggrgator.QuoteAggregator;
import com.ashish.aggrgator.TradeAggregator;

import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class App {

    public static void main(String[] args) {

         ExecutorService executorService = Executors.newCachedThreadPool(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread("Aggregator threads");
            }
        });

        TradeAggregator tradeAggregator = new TradeAggregator();
        QuoteAggregator quoteAggregator = new QuoteAggregator();

        executorService.submit(tradeAggregator);
        executorService.submit(quoteAggregator);

        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter to stop application...");
        tradeAggregator.closeStream();
        quoteAggregator.closeStream();


    }
}
