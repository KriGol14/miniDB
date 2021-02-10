 condition):
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

        # count the number of operations (<,> etc)
        no_of_ops = 0
        # this code is dumb on purpose... it needs to illustrate the underline technique
        # for each value in left column and right column, if condition, append the corresponding row to the new table
        for row_left in self.data:
            left_value = row_left[column_index_left]
            no_of_ops+=1
            join_table._insert(row_left)
            
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

        # count the number of operations (<,> etc)
        no_of_ops = 0
        # this code is dumb on purpose... it needs to illustrate the underline technique
        # for each value in left column and right column, if condition, append the corresponding row to the new table
        for row_right in table_right.data:
            right_value = row_right[column_index_right]
            no_of_ops+=1
            join_table.insert(row_right)
            
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

        # count the number of operations (<,> etc)
        no_of_ops = 0
        # this code is dumb on purpose... it needs to illustrate the underline technique
        # for each value in left column and right column, if condition, append the corresponding row to the new table
        for row_left in self.data:
            left_value = row_left[column_index_left]
            for row_right in table_right.data:
                right_value = row_right[column_index_right]
                no_of_ops+=1
                if (true): 
                    join_table._insert(row_left+row_right)

        print(f'## Select ops no. -> {no_of_ops}')
        print(f'# Left table size -> {len(self.data)}')
        print(f'# Right table size -> {len(table_right.data)}')

        return join_table
