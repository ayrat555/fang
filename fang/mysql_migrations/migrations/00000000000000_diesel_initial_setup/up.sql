-- This file was automatically created by Diesel to setup helper functions
-- and other internal bookkeeping. This file is safe to edit, any future
-- changes will be added to existing projects as new migrations.




-- Sets up a trigger for the given table to automatically set a column called
-- `updated_at` whenever the row is modified (unless `updated_at` was included
-- in the modified columns)
--
-- # Example
--
-- ```sql
-- CREATE TABLE users (id SERIAL PRIMARY KEY, updated_at TIMESTAMP NOT NULL DEFAULT NOW());
--
-- SELECT diesel_manage_updated_at('users');
-- ```

CREATE PROCEDURE diesel_manage_updated_at(IN _tbl VARCHAR(255))
BEGIN

    SET @sql = CONCAT('CREATE TRIGGER set_updated_at BEFORE UPDATE ON',_tbl,'
FOR EACH ROW
BEGIN
    if (NEW <> OLD
         OR NEW.updated_at <> OLD.updated_at)
    then
        SET NEW.updated_at = CURRENT_TIMESTAMP();
    end if;
END;');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END;