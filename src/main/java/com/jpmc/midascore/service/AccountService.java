package com.jpmc.midascore.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;

@Service
public class AccountService {

    private final UserRepository userRepository;

    @Autowired
    public AccountService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public double getBalance(int userId) {
        UserRecord user = userRepository.findById((long) userId);
        if (user == null) {
            return 0.0;
        }
        return user.getBalance();
    }
}
