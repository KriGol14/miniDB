# this file should be imported by the table.py file, so that its implementations work 
# import Database and Btree classes so that we can use their methods and attributes
from btree import Btree
import pandas as pd

# left outer join returns the matching rows as well as the rows which are in the left table but not in the right table
def _left_outer_join(self, table_right: Table, condition):
        '''
        Join table (left) with a supplied table (right) where condition is met.
        '''
        # get columns and operator
        column_name_left, operator, column_name_right = self._parse_condition(condition, join=True)
        # try to find both columns, if you fail raise error
        try:
            column_index_left = self.column_names.index(column_name_left)
            column_index_right = table_right.column_names.index(column_name_right)
        except:
            raise Exception(f'Columns dont exist in one or both tables.')

        # get the column names of both tables with the table name in front
        # ex. for left -> name becomes left_table_name_name etc
        left_names = [f'{self._name}_{colname}' for colname in self.column_names]
        right_names = [f'{table_right._name}_{colname}' for colname in table_right.column_names]

        # define the new tables name, its column names and types
        join_table_name = f'{self._name}__left_outer_join_{table_right._name}'
        join_table_colnames = left_names+right_names
        join_table_coltypes = self.column_types+table_right.column_types
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)          

        # in left_outer_join we need the matching rows of the two tables, as well as the values of the left_table that don't exist in the right_table
        # the values that do not exist in the right_table are replaced with null values
        # exists is a boolean variable which is True if the value of the left_table exists in the right_table too
        # count the number of operations (<,> etc)
        no_of_ops = 0
        null_values = []
        for row_left in self.data:
            null_values.clear()
            left_value = row_left[column_index_left]
            exists = False
            for row_right in table_right.data:
                right_value = row_right[column_index_right]
                no_of_ops+=1
                if get_op(operator, left_value, right_value): #EQ_OP
                    exists = True
            if exists == True:
                join_table._insert(row_left + row_right)
            elif exists == False:
                for i in range(table_right._no_of_columns):
                    null_values.append(None)
                join_table._insert(row_left + null_values)

        print(f'## Select ops no. -> {no_of_ops}')
        print(f'# Left table size -> {len(self.data)}')
        print(f'# Right table size -> {len(table_right.data)}')

        return join_table


def _right_outer_join(self, table_right: Table, condition):
        '''
        Join table (left) with a supplied table (right) where condition is met.
        '''
        # get columns and operator
        column_name_left, operator, column_name_right = self._parse_condition(condition, join=True)
        # try to find both columns, if you fail raise error
        try:
            column_index_left = self.column_names.index(column_name_left)
            column_index_right = table_right.column_names.index(column_name_right)
        except:
            raise Exception(f'Columns dont exist in one or both tables.')

        # get the column names of both tables with the table name in front
        # ex. for left -> name becomes left_table_name_name etc
        left_names = [f'{self._name}_{colname}' for colname in self.column_names]
        right_names = [f'{table_right._name}_{colname}' for colname in table_right.column_names]

        # define the new tables name, its column names and types
        join_table_name = f'{self._name}_right_outer_join_{table_right._name}'
        join_table_colnames = left_names+right_names
        join_table_coltypes = self.column_types+table_right.column_types
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)

        # in right_outer_join we need the matching rows of the two tables, as well as the values of the right_table that don't exist in the left_table 
        # the values that do not exist in the left_table are replaced with null values
        # exists is a boolean variable which is True if the value of the right_table exists in the left_table too
        # count the number of operations (<,> etc)
        no_of_ops = 0
        null_values = []
        for row_left in self.data:
            null_values.clear()
            left_value = row_left[column_index_left]
            exists = False
            for row_right in table_right.data:
                right_value = row_right[column_index_right]
                no_of_ops+=1
                if get_op(operator, left_value, right_value): #EQ_OP
                    exists = True
            if exists == True:
                join_table._insert(row_left + row_right)
            elif exists == False:
                for i in range(self._no_of_columns):
                    null_values.append(None)
                join_table._insert(row_right + null_values)
            
        print(f'## Select ops no. -> {no_of_ops}')
        print(f'# Left table size -> {len(self.data)}')
        print(f'# Right table size -> {len(table_right.data)}')

        return join_table


def _full_outer_join(self, table_right: Table, condition):
        '''
        Join table (left) with a supplied table (right) where condition is met.
        '''
        # get columns and operator
        column_name_left, operator, column_name_right = self._parse_condition(condition, join=True)
        # try to find both columns, if you fail raise error
        try:
            column_index_left = self.column_names.index(column_name_left)
            column_index_right = table_right.column_names.index(column_name_right)
        except:
            raise Exception(f'Columns dont exist in one or both tables.')

        # get the column names of both tables with the table name in front
        # ex. for left -> name becomes left_table_name_name etc
        left_names = [f'{self._name}_{colname}' for colname in self.column_names]
        right_names = [f'{table_right._name}_{colname}' for colname in table_right.column_names]

        # define the new tables name, its column names and types
        join_table_name = f'{self._name}__full_outer_join_{table_right._name}'
        join_table_colnames = left_names+right_names
        join_table_coltypes = self.column_types+table_right.column_types
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)

        # in order to perform outer_join we actually have to perform left_outer_join and right_outer_join
        # count the number of operations (<,> etc)
        no_of_ops = 0
        for row_left in self.data:
            null_values.clear()
            left_value = row_left[column_index_left]
            exists = False
            for row_right in table_right.data:
                right_value = row_right[column_index_right]
                no_of_ops+=1
                if get_op(operator, left_value, right_value): #EQ_OP
                    exists = True 
            # if records exist in both tables 
            if exists == True:
                join_table._insert(row_left+row_right)
            elif exists == False:
                null_values = []
                for i in range(table_right._no_of_columns):
                    null_values.append(None)
                join_table._insert(row_left + null_values)

        for row_left_new in self.data:
            null_values.clear()
            left_value_new = row_left_new[column_index_left]
            exists = False
            for row_right_new in table_right.data:
                right_value_new = row_right_new[column_index_right]
                no_of_ops+=1
                if get_op(operator, left_value_new, right_value_new): #EQ_OP
                    exists = True 
            # if records exist in both tables 
            if exists == True:
                join_table._insert(row_left_new + row_right_new)
            elif exists == False:
                null_values = []
                for i in range(self._no_of_columns):
                    null_values.append(None)
                join_table._insert(row_right_new + null_values)

        print(f'## Select ops no. -> {no_of_ops}')
        print(f'# Left table size -> {len(self.data)}')
        print(f'# Right table size -> {len(table_right.data)}')

        return join_table


def _inl_join(self, table_right: Table, condition):
        from database import Database
        '''
        Join table (left) with a supplied table (right) where condition is met.
        '''
        # get columns and operator
        column_name_left, operator, column_name_right = self._parse_condition(condition, join=True)
        # try to find both columns, if you fail raise error
        try:
            column_index_left = self.column_names.index(column_name_left)
            column_index_right = table_right.column_names.index(column_name_right)
        except:
            raise Exception(f'Columns dont exist in one or both tables.')

        # get the column names of both tables with the table name in front
        # ex. for left -> name becomes left_table_name_name etc
        left_names = [f'{self._name}_{colname}' for colname in self.column_names]
        right_names = [f'{table_right._name}_{colname}' for colname in table_right.column_names]

        # define the new tables name, its column names and types
        join_table_name = f'{self._name}__inl_join_{table_right._name}'
        join_table_colnames = left_names+right_names
        join_table_coltypes = self.column_types+table_right.column_types
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)

        # count the number of operations (<,> etc)
        no_of_ops = 0
        '''
        INDEX NESTED LOOP JOIN ALGORITHM
        '''
        # creating an index for the inner table
        index_name = f'{table_right._name}_index'
        create_index(self, table_right._name, index_name, index_type = 'Btree')
        # now for each value of the left table, we need to search in the index we created and check if it exists 
        # if the value we are searching exists, it should be appended to the join_table        
        # create an empty list to store in it the values that match 
        results = []

        for row_left in self.data:
            left_value = row_left[column_index_left]
        for left_value in self.data:
            no_of_ops += 1
            leaf_idx, ops = self._search(left_value, True)
            target_node = self.nodes[leaf_idx]
            # if the element we are searching for exists in the Btree, append in the list results[], else pass and return
            try:
                results.append(target_node.ptrs[target_node.values.index(left_value)])
                print("The value was found in the Btree")
                join_table._insert(results)
            except:
                print("Not found")
                pass 

        print(f'## Select ops no. -> {no_of_ops}')
        print(f'# Left table size -> {len(self.data)}')
        print(f'# Right table size -> {len(table_right.data)}')

        return join_table


    # 1) sort both tables on the condition given 
    # 2) merge-like concurrently scan the two tables
    # 3) return the join_table containing the matching rows, found through merging the tables
def _sm_join(self, table_right: Table, condition):
        '''
        Join table (left) with a supplied table (right) where condition is met.
        '''
        # get columns and operator
        column_name_left, operator, column_name_right = self._parse_condition(condition, join=True)
        # try to find both columns, if you fail raise error
        try:
            column_index_left = self.column_names.index(column_name_left)
            column_index_right = table_right.column_names.index(column_name_right)
        except:
            raise Exception(f'Columns dont exist in one or both tables.')

        # get the column names of both tables with the table name in front
        # ex. for left -> name becomes left_table_name_name etc
        left_names = [f'{self._name}_{colname}' for colname in self.column_names]
        right_names = [f'{table_right._name}_{colname}' for colname in table_right.column_names]

        # define the new tables name, its column names and types
        join_table_name = f'{self._name}_sm_join_{table_right._name}'
        join_table_colnames = left_names+right_names
        join_table_coltypes = self.column_types+table_right.column_types
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)

        # count the number of operations
        no_of_ops = 0
        # sort left table on column_name_left
        sorted_left = sorted(self, key=lambda self:self[column_name_left])
        # sort right table on column_name_right
        sorted_right = sorted(table_right, key=lambda table_right:table_right[column_name_right])

        # now that the tables are sorted we can merge the two tables using the pandas library 
        merged_table = pd.merge(sorted_left, sorted_right, left_on="column_name_left", right_on="column_name_right")
        for row_left in merged_table.data:
            for row_right in merged_table.data:
                no_of_ops += 1
                join_table._insert(row_left + row_right)
        
        print(f'## Select ops no. -> {no_of_ops}')
        print(f'# Left table size -> {len(self.data)}')
        print(f'# Right table size -> {len(table_right.data)}')
        return join_table
