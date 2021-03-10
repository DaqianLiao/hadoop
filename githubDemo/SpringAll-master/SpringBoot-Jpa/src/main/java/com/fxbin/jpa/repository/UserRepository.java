package com.fxbin.jpa.repository;

import com.fxbin.jpa.bean.User;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

/**
 * UserRespository
 *
 * @author fxbin
 * @version v1.0
 * @since 2019/2/27 23:47
 */
@Repository
public interface UserRepository extends CrudRepository<User, Integer> {
}
