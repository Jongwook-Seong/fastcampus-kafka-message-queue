package com.fastcampus.kafkahandson.service;

import com.fastcampus.kafkahandson.data.MyEntity;
import com.fastcampus.kafkahandson.data.MyJpaRepository;
import com.fastcampus.kafkahandson.event.MyCdcApplicationEvent;
import com.fastcampus.kafkahandson.model.MyModel;
import com.fastcampus.kafkahandson.model.MyModelConverter;
import com.fastcampus.kafkahandson.model.OperationType;
import com.fastcampus.kafkahandson.producer.MyCdcProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class MyServiceImpl implements MyService {

    private final MyJpaRepository myJpaRepository;
    private final MyCdcProducer myCdcProducer;
    private final ApplicationEventPublisher applicationEventPublisher;

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

    /** try-catch를 통한 방식은
     * DB 접근시 에러 발생 -> DB 롤백 성공, 카프카 접근 없음
     * 그러나 DB 접근 후 카프카 프로듀싱 후 에러 발생 -> DB 데이터는 롤백, 카프카 메세지는 저장되는 결함 존재
     **/
//    @Override
//    @Transactional
//    public MyModel save(MyModel model) { // C, U
//        // CREATE: id null (model은 DB에 insert 되기 전이라서 id가 null이다.)
//        // UPDATE: id not null
//        OperationType operationType = model.getId() == null ? OperationType.CREATE : OperationType.UPDATE;
//        MyEntity entity = myJpaRepository.save(MyModelConverter.toEntity(model));
//        try {
//            myCdcProducer.sendMessage(MyModelConverter.toMessage(
//                    entity.getId(), MyModelConverter.toModel(entity), operationType));
//        } catch (JsonProcessingException e) {
//            throw new RuntimeException("Error processing JSON for sendMessage", e);
//        }
//        return MyModelConverter.toModel(entity);
//    }
//
//    @Override
//    @Transactional
//    public void delete(Integer id) { // D
//        myJpaRepository.deleteById(id);
//        try {
//            myCdcProducer.sendMessage(MyModelConverter.toMessage(id, null, OperationType.DELETE));
//        } catch (JsonProcessingException e) {
//            throw new RuntimeException("Error processing JSON for sendMessage", e);
//        }
//    }

    /**
     * 이 방식은 @TransactionalEventListener 부분에서 예외 발생해도 DB 롤백이 되지 않음
     * 왜냐하면 @Transactional 부분 수행으로 DB 커밋이 완료된 이후 트랜잭션과는 별개로 수행되기 때문
     * 물론 프로듀스 완료 후 예외 발생하면 문제는 없지만, 프로듀스 이전에 예외 발생하면 데이터 정합성이 깨짐
     * + 일반적으로는 Kafka Produce 로직이 맨 마지막에 위치한다면 그 전에 어떤 exception이 발생하든 의도된 동작에 문제 없겠지만,
     * JPA를 사용함에 따라 transactional write-behind가 적용되어 엔티티의 id 채번이 필요한 경우가 아닌 경우에는 Kafka Produce 이후에 DB Command가 지연되어 실행됨
     * 따라서 DB(command)단에서 문제가 발생할 수 있는 가능성이 있다면, 결과적으로 DB에는 미반영되고 Kafka Produce만 정상적으로 이루어질 수도 있게 됨
     * - transactional write-behind : JPA에서 쿼리의 실질적 실행은 트랜잭션 종료(메서드 return) 이후 동작
     */
//    @Override
//    @Transactional
//    public MyModel save(MyModel model) {
//        OperationType operationType = model.getId() == null ? OperationType.CREATE : OperationType.UPDATE;
//        MyEntity entity = myJpaRepository.save(MyModelConverter.toEntity(model));
//        MyModel resultModel = MyModelConverter.toModel(entity);
//        applicationEventPublisher.publishEvent(
//                new MyCdcApplicationEvent(this, entity.getId(), resultModel, operationType));
//        return resultModel;
//    }
//
//    @Override
//    @Transactional
//    public void delete(Integer id) {
//        myJpaRepository.deleteById(id);
//        applicationEventPublisher.publishEvent(
//                new MyCdcApplicationEvent(this, id, null, OperationType.DELETE));
//    }

    /**
     * @EntityListeners 활용
     */
    @Override
    @Transactional
    public MyModel save(MyModel model) {
        OperationType operationType = model.getId() == null ? OperationType.CREATE : OperationType.UPDATE;
        MyEntity entity = myJpaRepository.save(MyModelConverter.toEntity(model));
        MyModel resultModel = MyModelConverter.toModel(entity);
        return resultModel;
    }

    @Override
    @Transactional
    public void delete(Integer id) {
        myJpaRepository.deleteById(id);
    }
}
