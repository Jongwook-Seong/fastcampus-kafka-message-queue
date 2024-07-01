package com.fastcampus.kafkahandson.service;

import com.fastcampus.kafkahandson.data.MyEntity;
import com.fastcampus.kafkahandson.data.MyJpaRepository;
import com.fastcampus.kafkahandson.model.MyModel;
import com.fastcampus.kafkahandson.model.MyModelConverter;
import com.fastcampus.kafkahandson.model.OperationType;
import com.fastcampus.kafkahandson.producer.MyCdcProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class MyServiceImpl implements MyService {

    private final MyJpaRepository myJpaRepository;
    private final MyCdcProducer myCdcProducer;

    /** READ는 CUD에 속하지 않으므로 CDC 적용에서 제외 **/
    @Override
    public List<MyModel> findAll() {
        List<MyEntity> entities = myJpaRepository.findAll();
        return entities.stream().map(MyModelConverter::toModel).toList();
    }

    @Override
    public MyModel findById(Integer id) {
        Optional<MyEntity> entity = myJpaRepository.findById(id);
        return entity.map(MyModelConverter::toModel).orElse(null);
    }

    @Override
    @Transactional
    public MyModel save(MyModel model) { // C, U
        // CREATE: id null (model은 DB에 insert 되기 전이라서 id가 null이다.)
        // UPDATE: id not null
        OperationType operationType = model.getId() == null ? OperationType.CREATE : OperationType.UPDATE;
        MyEntity entity = myJpaRepository.save(MyModelConverter.toEntity(model));
        try {
            myCdcProducer.sendMessage(MyModelConverter.toMessage(
                    entity.getId(), MyModelConverter.toModel(entity), operationType));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error processing JSON for sendMessage", e);
        }
        return MyModelConverter.toModel(entity);
    }

    @Override
    @Transactional
    public void delete(Integer id) { // D
        myJpaRepository.deleteById(id);
        try {
            myCdcProducer.sendMessage(MyModelConverter.toMessage(id, null, OperationType.DELETE));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error processing JSON for sendMessage", e);
        }
    }
}
