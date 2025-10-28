package com.jpmc.midascore.controller;

import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;

@RestController
@RequestMapping("/balance")
public class BalanceController {

    @Autowired
    private UserRepository userRepository;

    @GetMapping
    public Balance getBalance(@RequestParam("userId") int userId) {
        UserRecord user = userRepository.findById(userId);

        float amount = 0.0f;
        if (user != null) {
            amount = user.getBalance();
        }

        return new Balance(amount);
    }
}
