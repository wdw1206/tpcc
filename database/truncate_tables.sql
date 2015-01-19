SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

truncate table customer;
truncate table district;
truncate table history;
truncate table item;
truncate table new_orders;
truncate table order_line;
truncate table orders;
truncate table stock;
truncate table warehouse;


SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
