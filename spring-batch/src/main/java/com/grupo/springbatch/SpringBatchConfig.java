package com.grupo.springbatch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {
     @Autowired
     private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private ItemWriter<BankTransaction> bankTransactionItemWriter;
    @Autowired
    private ItemProcessor<BankTransaction,BankTransaction> bankTransactionitemProcessor;

    @Bean
    public Job bankjob(){
        Step step1= stepBuilderFactory.get("step")
                .<BankTransaction,BankTransaction>chunk(100)
                .reader(getItemReader())
                .processor(bankTransactionitemProcessor)
                .writer(bankTransactionItemWriter)
                .build();

        return jobBuilderFactory.get("load-job")
                .start(step1)
                .build();
    }


    @Bean
    public FlatFileItemReader<BankTransaction> getItemReader(){
        FlatFileItemReader<BankTransaction> flatFileItemReader=new FlatFileItemReader<>();
        flatFileItemReader.setName("FFIR");
        flatFileItemReader.setLinesToSkip(1);
        flatFileItemReader.setResource(new ClassPathResource("data.csv"));
        flatFileItemReader.setLineMapper(lineMapper());
        return flatFileItemReader;
    }

    /**
     *
     * Pour parcourir le fichIer ce linMapper va utiliser admis le job builder
     * @return
     */
    @Bean
    public LineMapper<BankTransaction> lineMapper(){
        DefaultLineMapper<BankTransaction> lineMapper = new DefaultLineMapper<BankTransaction>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(new String[]{"id","accountId","strTransactionDate","transactionType","amount"});
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        BeanWrapperFieldSetMapper<BankTransaction> fieldSetMapper = new BeanWrapperFieldSetMapper<BankTransaction>();
        fieldSetMapper.setTargetType(BankTransaction.class);
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }












}
