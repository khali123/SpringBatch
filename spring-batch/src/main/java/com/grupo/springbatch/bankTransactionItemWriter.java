package com.grupo.springbatch;

import org.springframework.batch.item.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class bankTransactionItemWriter implements ItemWriter<BankTransaction> {
    @Autowired
    private BankTransactionRepository bankTransactionRepository;

    @Override
    public void write(List<? extends BankTransaction> list) throws Exception {
          bankTransactionRepository.saveAll(list);
    }

    /***
     *
     */
}
