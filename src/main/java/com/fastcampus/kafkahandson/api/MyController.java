package com.fastcampus.kafkahandson.api;

import com.fastcampus.kafkahandson.model.MyMessage;
import com.fastcampus.kafkahandson.model.MyModel;
import com.fastcampus.kafkahandson.producer.MyProducer;
import com.fastcampus.kafkahandson.producer.MySCStreamProducer;
import com.fastcampus.kafkahandson.producer.MySecondProducer;
import com.fastcampus.kafkahandson.service.MyService;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RequiredArgsConstructor
@RestController
public class MyController {

////    private final MySCStreamProducer mySCStreamProducer;
//    private final MyProducer myProducer;
//    private final MySecondProducer mySecondProducer;
//
//    @RequestMapping("/hello")
//    String hello() {
//        return "Hello World";
//    }
//
//    @PostMapping("/message")
//    void message(@RequestBody MyMessage message) {
////        mySCStreamProducer.sendMessage(message);
//        try {
//            myProducer.sendMessage(message);
//        } catch (JsonProcessingException e) {
//            e.fillInStackTrace();
//        }
//    }
//
////    @PostMapping("/second-message/{key}")
//    void message(@PathVariable String key, @RequestBody String message) {
//        mySecondProducer.sendMessageWithKey(key, message);
//    }

    private final MyService myService;

    /** CRUD HttpStatus를 위한 상세한 핸들링 생략 **/
    @PostMapping("/greeting")
    MyModel create(@RequestBody Request request) {
        if (request == null || request.userId == null || request.userAge == null || request.userName == null || request.content == null) {
            return null;
        }
        MyModel myModel = MyModel.create(request.userId, request.userAge, request.userName, request.content);
        return myService.save(myModel);
    }

    @GetMapping("/greetings/{id}")
    MyModel get(@PathVariable Integer id) {
        return myService.findById(id);
    }

    @PatchMapping("/greetings/{id}")
    MyModel update(@PathVariable Integer id, @RequestBody String content) {
        if (id == null || content == null || content.isBlank())
            return null;
        MyModel myModel = myService.findById(id);
        myModel.setContent(content);
        return myService.save(myModel);
    }

    @DeleteMapping("/greetings/{id}")
    void delete(@PathVariable Integer id) {
        myService.delete(id);
    }

    @Data
    private static class Request {
        Integer userId;
        Integer userAge;
        String userName;
        String content;
    }
}
