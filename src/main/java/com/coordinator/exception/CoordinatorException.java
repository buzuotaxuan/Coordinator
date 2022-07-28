package com.coordinator.exception;

/**
 * @author Devy
 * @Description TODO
 * @createTime 2022年07月14日 21:50:00
 */
public class CoordinatorException extends RuntimeException{

  private String code;
  private String msg;

  public CoordinatorException(){
    super();
  }

  public CoordinatorException(String msg){
    super(msg);
    this.msg=msg;
  }
  public CoordinatorException(String code,String msg){
    super(msg);
    this.code=code;
    this.msg=msg;
  }
}
