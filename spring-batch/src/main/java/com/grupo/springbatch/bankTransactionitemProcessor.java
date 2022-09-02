package com.grupo.springbatch;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;

@Component
public class bankTransactionitemProcessor implements ItemProcessor<BankTransaction,BankTransaction> {

    private SimpleDateFormat dateFormat=new SimpleDateFormat("dd/MM/YYYY-HH:mm");

    @Override
    public BankTransaction process(BankTransaction bankTransaction) throws Exception {
        bankTransaction.setTransactionDate(dateFormat.parse(bankTransaction.getStrTransactionDate()));
        return bankTransaction;
    }


}
