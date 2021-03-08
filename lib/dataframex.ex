defmodule Dataframex do

	#===============================================================================
	#===============================================================================
	#	pandas compatible
	#===============================================================================
	#===============================================================================
	@doc """
	Read/write dataframe from csv file

	## Examples
		iex> Dataframex.write_csv( %{ "columns" => [ "c3","c4","c1","c2" ], "rows" => [ [ 0, 9, 1, 2 ], [ 0, 9, 6, 7 ], [ 0, 9, 11, 12 ] ] }, "test/dataframe.csv" )
		iex> Dataframex.read_csv( "test/dataframe.csv" )
		%{ "columns" => [ "c3","c4","c1","c2" ], "rows" => [ [ "0","9","1","2" ], [ "0","9","6","7" ], [ "0","9","11","12" ] ] }
		iex> Dataframex.read_csv( "test/dataframe.csv", 2 )
		%{ "columns" => [ "c3", "c4", "c1", "c2" ], "rows" => [ [ "0", "9", "1", "2"] ] }
		#iex> File.rm!( "test/dataframe.csv" )
		#:ok
	"""
	def read_csv( path, head \\ -1 ), do: read_from_csv_file( path, head )
	def write_csv( dataframe, path ), do: write_to_csv_file( dataframe, path )

	@doc """
	Get columns from dataframe

	## Examples
		iex> Dataframex.columns( %{ "columns" => [ "c3","c4","c1","c2" ], "rows" => [ [ 0, 9, 1, 2 ], [ 0, 9, 6, 7 ], [ 0, 9, 11, 12 ] ] } )
		[ "c3","c4","c1","c2" ]
	"""
	def columns( dataframe ), do: dataframe[ "columns" ]

	#===============================================================================
	#===============================================================================
	#	NumPy compatible
	#===============================================================================
	#===============================================================================
	@doc """
	Transpose rows

	## Examples
		iex> Dataframex.transpose( [ [ "c0", "c1", "c2" ], [ "j1", "j2", "j3" ] ] )
		[ [ "c0", "j1" ], [ "c1", "j2" ], [ "c2", "j3" ] ]
	"""
	def transpose( rows ) do
		rows
		|> Enum.zip
		|> Enum.map( & Tuple.to_list( &1 ) )
	end

	#===============================================================================
	#===============================================================================
	#	Original
	#===============================================================================
	#===============================================================================

	@doc """
	Types from rows

	## Examples
		iex> Dataframex.types_from_row( %{ "columns" => [ "c1", "c2", "c3", "c4" ], "rows" => [ [ "abc", 123, 12.34, true ] ] } )
		[ "String", "Integer", "Float", "Boolean" ]
		iex> Dataframex.types_from_row( %{ "columns" => [ "c1", "c2", "c3", "c4" ], "rows" => [ [ "abc", "123", "12.34", "true" ] ] } )
		[ "String", "Integer", "Float", "Boolean" ]

		iex> Dataframex.types_from_row( %{ "columns" => [ "c1", "c2", "c3", "c4" ], "rows" => [ [ "abc", "123", "12.34", "true" ], [ "456", "56.78", "12.34", "1" ], [ "789", "90.12", "xyz", "false" ], [ "012", "12.34", "12.34", "true" ] ] } )
		[ "Integer", "Float", "Float", "Boolean" ]
	"""
	def types_from_row(  %{ "columns" => _columns, "rows" => rows } = _dataframe ) do
		rows
		|> Dataframex.transpose
		|> Enum.map( fn columns -> columns
			|> Enum.reduce( [], fn column, acc -> [ Type.is( column ) | acc ] end )
			|> Enum.reverse
			|> Enum.group_by( & &1, & &1 )
			|> Enum.map( fn { k, v } -> { k, Enum.count( v ) } end )
			|> Enum.sort( & elem( &1, 1 ) > elem( &2, 1 ) )
			|> List.first
			|> elem( 0 )
		end )
	end

	@doc """
	Pickup match input-key/join-key

	## Examples
		iex> Dataframex.pickup_match_key( [ "c0", "c1", "c2", "c3", "c4", "c5" ], [ "j1", "j2", "j3", "j4", "j5", "j6" ], [ "c1", "j1", "c3", "j3", "c5", "j5" ] )
		%{ "input_key_no1" => 1, "join_key_no1" => 0, "input_key_no2" => 3, "join_key_no2" => 2, "input_key_no3" => 5, "join_key_no3" => 4 }
	"""
	def pickup_match_key( columns, join_columns, options ) do
		input_key_no1 = Enum.find_index( columns, & &1 == Enum.at( options, 0 ) )
		join_key_no1  = Enum.find_index( join_columns, & &1 == Enum.at( options, 1 ) )

		input_key_no2 = Enum.find_index( columns, & &1 == Enum.at( options, 2 ) )
		join_key_no2  = Enum.find_index( join_columns, & &1 == Enum.at( options, 3 ) )

		input_key_no3 = Enum.find_index( columns, & &1 == Enum.at( options, 4 ) )
		join_key_no3  = Enum.find_index( join_columns, & &1 == Enum.at( options, 5 ) )

		%{ "input_key_no1" => input_key_no1, "join_key_no1" => join_key_no1, "input_key_no2" => input_key_no2, "join_key_no2" => join_key_no2, "input_key_no3" => input_key_no3, "join_key_no3" => join_key_no3, }
	end

	@doc """
	Filter match row

	## Examples
		iex> Dataframex.filter_match_row( [ [ "l0", "l1", "l2" ], [ "l4", "l5", "l6" ] ], [ [ "l4", "r2" ], [ "l0", "r6" ] ], %{ "input_key_no1" => 0, "join_key_no1" => 0 } )
		[ %{ "input" => [ "l0", "l1", "l2" ], "join" => [ "l0", "r6" ] }, %{ "input" => [ "l4", "l5", "l6" ], "join" => [ "l4", "r2" ] } ]

		iex> Dataframex.filter_match_row( [ [ "l0", "l1", "l2" ], [ "l4", "l5", "l6" ] ], [ [ "l4", "r2" ], [ "r5", "r6" ] ], %{ "input_key_no1" => 0, "join_key_no1" => 0 } )
		[ %{ "input" => [ "l4", "l5", "l6" ], "join" => [ "l4", "r2" ] } ]

		iex> Dataframex.filter_match_row( [ [ "l0", "l1", "l2" ], [ "l4", "l5", "l6" ] ], [ [ "r1", "r2" ], [ "r5", "r6" ] ], %{ "input_key_no1" => 0, "join_key_no1" => 0 } )
		[]

		iex> Dataframex.filter_match_row( [ [ "l0", "l1", "l2", "r3" ], [ "l4", "l5", "l6", "r7" ], [ "r8", "r9", "r10", "r11" ] ], [ [ "l1", "j2", "j3" ], [ "j5", "j6", "j7" ], [ "r9", "j10", "j11" ] ], %{ "input_key_no1" => 1, "join_key_no1" => 0 } )
		[ %{ "input" => [ "l0", "l1", "l2", "r3" ], "join" => [ "l1", "j2", "j3" ] }, %{ "input" => [ "r8", "r9", "r10", "r11" ], "join" => [ "r9", "j10", "j11" ] } ]

		iex> Dataframex.filter_match_row( [ [ "l0", "l1", "l2", "r3" ], [ "l4", "l5", "l6", "r7" ], [ "r8", "r9", "r10", "r11" ] ], [ [ "l1", "j2", "l2" ], [ "l5", "j6", "l6" ], [ "j9", "j10", "j11" ] ], %{ "input_key_no1" => 1, "join_key_no1" => 0, "input_key_no2" => 2, "join_key_no2" => 2 } )
		[ %{ "input" => [ "l0", "l1", "l2", "r3" ], "join" => [ "l1", "j2", "l2" ] }, %{ "input" => [ "l4", "l5", "l6", "r7" ], "join" => [ "l5", "j6", "l6" ] } ]

		iex> Dataframex.filter_match_row( [ [ "l0", "l1", "l2", "r3" ], [ "l4", "l5", "l6", "l6" ], [ "r8", "r9", "r10", "r11" ] ], [ [ "l1", "j2", "l2" ], [ "l5", "j6", "l6" ], [ "j9", "j10", "j11" ] ], %{ "input_key_no1" => 1, "join_key_no1" => 0, "input_key_no2" => 2, "join_key_no2" => 2, "input_key_no3" => 3, "join_key_no3" => 2 } )
		[ %{ "input" => [ "l4", "l5", "l6", "l6" ], "join" => [ "l5", "j6", "l6" ] } ]
	"""
	def filter_match_row( input_rows, join_rows, key_nos ) do
		input_rows
		|> Enum.map( fn input_row ->
			join_rows |> Enum.map( fn join_row ->
#IO.puts "-----------------------------------------"
#IO.inspect key_nos[ "input_key_no1" ]
#IO.inspect key_nos[ "join_key_no1" ]
#IO.inspect Enum.at( input_row, key_nos[ "input_key_no1" ] )
#IO.inspect Enum.at( join_row, key_nos[ "join_key_no1" ] )
#IO.puts "-----------------------------------------"
				if(
					( Enum.at( input_row, key_nos[ "input_key_no1" ] ) == Enum.at( join_row, key_nos[ "join_key_no1" ] ) )
					&& ( key_nos[ "input_key_no2" ] == nil || ( Enum.at( input_row, key_nos[ "input_key_no2" ] ) == Enum.at( join_row, key_nos[ "join_key_no2" ] ) ) )
					&& ( key_nos[ "input_key_no3" ] == nil || ( Enum.at( input_row, key_nos[ "input_key_no3" ] ) == Enum.at( join_row, key_nos[ "join_key_no3" ] ) ) )
				)
				do
					%{ "input" => input_row, "join" => join_row }
				end
			end )
		end )
		|> Enum.flat_map( & &1 )
		|> Enum.filter( & &1 != nil )
	end

	@doc """
	Filter input exists row

	## Examples
		iex> Dataframex.filter_input_exist_row( [ [ "l0", "l1", "l2" ], [ "l4", "l5", "l6" ] ], [ [ "l4", "r2" ], [ "l0", "r6" ] ], %{ "input_key_no1" => 0, "join_key_no1" => 0 } )
		[ %{ "input" => [ "l0", "l1", "l2" ], "join" => [ "l0", "r6" ] }, %{ "input" => [ "l4", "l5", "l6" ], "join" => [ "l4", "r2" ] } ]

		iex> Dataframex.filter_input_exist_row( [ [ "l0", "l1", "l2" ], [ "l4", "l5", "l6" ] ], [ [ "l4", "r2" ], [ "r5", "r6" ] ], %{ "input_key_no1" => 0, "join_key_no1" => 0 } )
		[ %{ "input" => [ "l0", "l1", "l2" ], "join" => [ "", "" ] }, %{ "input" => [ "l4", "l5", "l6" ], "join" => [ "l4", "r2" ] } ]

		iex> Dataframex.filter_input_exist_row( [ [ "l0", "l1", "l2" ], [ "l4", "l5", "l6" ] ], [ [ "r1", "r2" ], [ "r5", "r6" ] ], %{ "input_key_no1" => 0, "join_key_no1" => 0 } )
		[ %{ "input" => [ "l0", "l1", "l2" ], "join" => [ "", "" ] }, %{ "input" => [ "l4", "l5", "l6" ], "join" => [ "", "" ] } ]

		iex> Dataframex.filter_input_exist_row( [ [ "l0", "l1", "l2" ], [ "l4", "l5", "l6" ] ], [ [ "l0", "r2" ], [ "l0", "r6" ] ], %{ "input_key_no1" => 0, "join_key_no1" => 0 } )
		[ %{ "input" => [ "l0", "l1", "l2" ], "join" => [ "l0", "r2" ] }, %{ "input" => [ "l0", "l1", "l2" ], "join" => [ "l0", "r6" ] }, %{ "input" => [ "l4", "l5", "l6" ], "join" => [ "", "" ] } ]

		iex> Dataframex.filter_input_exist_row( [ [ "l0", "l1", "l2", "r3" ], [ "l4", "l5", "l6", "r7" ], [ "r8", "r9", "r10", "r11" ] ], [ [ "l1", "j2", "j3" ], [ "j5", "j6", "j7" ], [ "r9", "j10", "j11" ] ], %{ "input_key_no1" => 1, "join_key_no1" => 0 } )
		[ %{ "input" => [ "l0", "l1", "l2", "r3" ], "join" => [ "l1", "j2", "j3" ] }, %{ "input" => [ "l4", "l5", "l6", "r7" ], "join" => [ "", "", "" ] }, %{ "input" => [ "r8", "r9", "r10", "r11" ], "join" => [ "r9", "j10", "j11" ] } ]

		iex> Dataframex.filter_input_exist_row( [ [ "l0", "l1", "l2", "r3" ], [ "l4", "l5", "l6", "r7" ], [ "r8", "r9", "r10", "r11" ] ], [ [ "l1", "j2", "j3" ], [ "l1", "j6", "j7" ], [ "r9", "j10", "j11" ] ], %{ "input_key_no1" => 1, "join_key_no1" => 0 } )
		[ %{ "input" => [ "l0", "l1", "l2", "r3" ], "join" => [ "l1", "j2", "j3" ] }, %{ "input" => [ "l0", "l1", "l2", "r3" ], "join" => [ "l1", "j6", "j7" ] }, %{ "input" => [ "l4", "l5", "l6", "r7" ], "join" => [ "", "", "" ] }, %{ "input" => [ "r8", "r9", "r10", "r11" ], "join" => [ "r9", "j10", "j11" ] } ]

		#iex> Dataframex.filter_input_exist_row( [ [ "l0", "l1", "l2", "r3" ], [ "l4", "l5", "l6", "r7" ], [ "r8", "r9", "r10", "r11" ] ], [ [ "l1", "j2", "l2" ], [ "l5", "j6", "l6" ], [ "j9", "j10", "j11" ] ], %{ "input_key_no1" => 1, "join_key_no1" => 0, "input_key_no2" => 2, "join_key_no2" => 2 } )
		#[ %{ "input" => [ "l0", "l1", "l2", "r3" ], "join" => [ "l1", "j2", "j3" ] }, %{ "input" => [ "l4", "l5", "l6", "r7" ], "join" => [ "l5", "j6", "l6" ] }, %{ "input" => [ "r8", "r9", "r10", "r11" ], "join" => [ "", "", "" ] } ]

		#iex> Dataframex.filter_input_exist_row( [ [ "l0", "l1", "l2", "r3" ], [ "l4", "l5", "l6", "l6" ], [ "r8", "r9", "r10", "r11" ] ], [ [ "l1", "j2", "l2" ], [ "l5", "j6", "l6" ], [ "j9", "j10", "j11" ] ], %{ "input_key_no1" => 1, "join_key_no1" => 0, "input_key_no2" => 2, "join_key_no2" => 2, "input_key_no3" => 3, "join_key_no3" => 2 } )
		#[ %{ "input" => [ "l4", "l5", "l6", "l6" ], "join" => [ "l5", "j6", "l6" ] } ]
	"""
	def filter_input_exist_row( input_rows, join_rows, key_nos ) do
		result = input_rows
			|> Enum.map( fn input_row ->
				pickup = join_rows |> Enum.map( fn join_row ->
#IO.puts "-------------------------------"
#IO.inspect key_nos[ "input_key_no1" ]
#IO.inspect key_nos[ "join_key_no1" ]
#IO.inspect Enum.at( input_row, key_nos[ "input_key_no1" ] )
#IO.inspect Enum.at( join_row, key_nos[ "join_key_no1" ] )
#IO.inspect Enum.at( input_row, key_nos[ "input_key_no1" ] ) == Enum.at( join_row, key_nos[ "join_key_no1" ] )
#IO.puts "-------------------------------"
#IO.inspect key_nos[ "join_key_no1" ]
#IO.inspect Enum.at( join_row, key_nos[ "join_key_no1" ] )
#IO.puts "-------------------------------"
					if(
						( Enum.at( input_row, key_nos[ "input_key_no1" ] ) == Enum.at( join_row, key_nos[ "join_key_no1" ] ) || Enum.at( join_row, key_nos[ "join_key_no1" ] ) == nil  )
						&& ( key_nos[ "input_key_no2" ] == nil || Enum.at( input_row, key_nos[ "input_key_no2" ] ) == Enum.at( join_row, key_nos[ "join_key_no2" ] ) || Enum.at( join_row, key_nos[ "join_key_no2" ] ) == nil )
						&& ( key_nos[ "input_key_no3" ] == nil || Enum.at( input_row, key_nos[ "input_key_no3" ] ) == Enum.at( join_row, key_nos[ "join_key_no3" ] ) || Enum.at( join_row, key_nos[ "join_key_no3" ] ) == nil  )
					)
					do
						%{ "input" => input_row, "join" => join_row }
					end
				end )

				if pickup |> Enum.filter( & &1 != nil ) == [] do
					[ %{ "input" => input_row, "join" => nil } ]
				else
					pickup
				end
			end )
			|> Enum.flat_map( & &1 )
			|> Enum.filter( & &1 != nil )

		empty_count = join_rows
			|> List.first
			|> Enum.count

		result
		|> Enum.map( & if &1[ "join" ] == nil, do: Map.put( &1, "join", List.duplicate( "", empty_count ) ), else: &1 )
	end

	@doc """
	Filter join exists row

	## Examples
		iex> Dataframex.filter_join_exist_row( [ [ "l0", "l1", "l2" ], [ "l4", "l5", "l6" ] ], [ [ "l4", "r2" ], [ "l0", "r6" ] ], %{ "input_key_no1" => 0, "join_key_no1" => 0 } )
		[ %{ "input" => [ "l4", "l5", "l6" ], "join" => [ "l4", "r2" ] }, %{ "input" => [ "l0", "l1", "l2" ], "join" => [ "l0", "r6" ] } ]

		iex> Dataframex.filter_join_exist_row( [ [ "l0", "l1", "l2" ], [ "l4", "l5", "l6" ] ], [ [ "l4", "r2" ], [ "r5", "r6" ] ], %{ "input_key_no1" => 0, "join_key_no1" => 0 } )
		[ %{ "input" => [ "l4", "l5", "l6" ], "join" => [ "l4", "r2" ] }, %{ "input" => [ "", "", "" ], "join" => [ "r5", "r6" ] } ]

		iex> Dataframex.filter_join_exist_row( [ [ "l0", "l1", "l2" ], [ "l4", "l5", "l6" ] ], [ [ "r1", "r2" ], [ "r5", "r6" ] ], %{ "input_key_no1" => 0, "join_key_no1" => 0 } )
		[ %{ "input" => [ "", "", "" ], "join" => [ "r1", "r2" ] }, %{ "input" => [ "", "", "" ], "join" => [ "r5", "r6" ] } ]

		#iex> Dataframex.filter_join_exist_row( [ [ "l0", "l1", "l2", "r3" ], [ "l4", "l5", "l6", "r7" ], [ "r8", "r9", "r10", "r11" ] ], [ [ "l1", "j2", "j3" ], [ "j5", "j6", "j7" ], [ "r9", "j10", "j11" ] ], %{ "input_key_no1" => 1, "join_key_no1" => 0 } )
		#[ %{ "input" => [ "l0", "l1", "l2", "r3" ], "join" => [ "l1", "j2", "j3" ] }, %{ "input" => [ "r8", "r9", "r10", "r11" ], "join" => [ "r9", "j10", "j11" ] } ]

		#iex> Dataframex.filter_join_exist_row( [ [ "l0", "l1", "l2", "r3" ], [ "l4", "l5", "l6", "r7" ], [ "r8", "r9", "r10", "r11" ] ], [ [ "l1", "j2", "l2" ], [ "l5", "j6", "l6" ], [ "j9", "j10", "j11" ] ], %{ "input_key_no1" => 1, "join_key_no1" => 0, "input_key_no2" => 2, "join_key_no2" => 2 } )
		#[ %{ "input" => [ "l0", "l1", "l2", "r3" ], "join" => [ "l1", "j2", "l2" ] }, %{ "input" => [ "l4", "l5", "l6", "r7" ], "join" => [ "l5", "j6", "l6" ] } ]

		#iex> Dataframex.filter_join_exist_row( [ [ "l0", "l1", "l2", "r3" ], [ "l4", "l5", "l6", "l6" ], [ "r8", "r9", "r10", "r11" ] ], [ [ "l1", "j2", "l2" ], [ "l5", "j6", "l6" ], [ "j9", "j10", "j11" ] ], %{ "input_key_no1" => 1, "join_key_no1" => 0, "input_key_no2" => 2, "join_key_no2" => 2, "input_key_no3" => 3, "join_key_no3" => 2 } )
		#[ %{ "input" => [ "l4", "l5", "l6", "l6" ], "join" => [ "l5", "j6", "l6" ] } ]
	"""
	def filter_join_exist_row( input_rows, join_rows, key_nos ) do
		result = join_rows
			|> Enum.map( fn join_row ->
				pickup = input_rows |> Enum.map( fn input_row ->
					if(
						( Enum.at( input_row, key_nos[ "input_key_no1" ] ) == Enum.at( join_row, key_nos[ "join_key_no1" ] ) || Enum.at( input_row, key_nos[ "input_key_no1" ] ) == nil )
						&& ( key_nos[ "input_key_no2" ] == nil || Enum.at( input_row, key_nos[ "input_key_no2" ] ) == Enum.at( join_row, key_nos[ "join_key_no2" ] ) || Enum.at( input_row, key_nos[ "input_key_no2" ] ) == nil )
						&& ( key_nos[ "input_key_no3" ] == nil || Enum.at( input_row, key_nos[ "input_key_no3" ] ) == Enum.at( join_row, key_nos[ "join_key_no3" ] ) || Enum.at( input_row, key_nos[ "input_key_no3" ] ) == nil )
					)
					do
						%{ "input" => input_row, "join" => join_row }
					end
				end )

				if pickup |> Enum.filter( & &1 != nil ) == [] do
					[ %{ "input" => nil, "join" => join_row } ]
				else
					pickup
				end
			end )
			|> Enum.flat_map( & &1 )
			|> Enum.filter( & &1 != nil )

		empty_count = input_rows
			|> List.first
			|> Enum.count

		result
		|> Enum.map( & if &1[ "input" ] == nil, do: Map.put( &1, "input", List.duplicate( "", empty_count ) ), else: &1 )
	end

	@doc """
	Unshift columns

	## Examples
		#iex> Dataframex.unshift_columns( [ "c0", "c1" ], [ "j1", "j2" ] )  #TODO: 未実装なので追って実装すること
		#[ "j2", "j1", "c0", "c1" ]
	"""
	def unshift_columns( columns, destination ) do
		Enum.reverse( destination ) ++ columns
	end

	@doc """
	Unshift rows

	## Examples
		iex> Dataframex.unshift_rows( [ %{ "input" => [ "l0", "l1" ], "join" => [ "j1", "j2", "j3" ] }, %{ "input" => [ "l4", "l5" ], "join" => [ "j4", "j5", "j6" ] }, %{ "input" => [ "r8", "r9" ], "join" => [ "j7", "j8", "j9" ] } ], [ 2, 0 ] )
		[ [ "j3", "j1", "l0", "l1" ], [ "j6", "j4", "l4", "l5" ], [ "j9", "j7", "r8", "r9" ] ]
	"""
	def unshift_rows( rows, column_nos ) do
		rows
		|> Enum.map( fn join_only_row ->
			( column_nos |> Enum.reduce( [], fn column_no, acc ->
				acc ++ [ Enum.at( join_only_row[ "join" ], column_no ) ]
			end ) ) ++ join_only_row[ "input" ]
		end )
	end

	#-----------------------------------------------------------------------
	#	Join
	#-----------------------------------------------------------------------

	@doc """
	Join when matched

	## Examples
		iex> Dataframex.join_when_matched( %{ "columns" => [ "c1", "c2" ], "rows" => [ [ "1", "2" ], [ "3", "4" ], [ "5", "6" ] ] }, %{ "source" => "test/Dataframe_join.csv", "destination" => [ "j1", "j3" ], "options" => [ "c2", "j2", ] } )
		%{ "columns" => [ "j3", "j1", "c1", "c2" ], "rows" => [ [ "3", "1", "1", "2" ], [ "7", "5", "5", "6" ] ] }
		iex> Dataframex.join_when_matched( %{ "columns" => [ "c1", "c2" ], "rows" => [ [ "1", "2" ], [ "3", "4" ], [ "5", "6" ], [ "7", "8" ], [ "9", "10" ] ] }, %{ "source" => "test/dataframe_join.csv", "destination" => [ "j1", "j3" ], "options" => [ "c2", "j2", ] } )
		%{ "columns" => [ "j3", "j1", "c1", "c2" ], "rows" => [ [ "3", "1", "1", "2" ], [ "7", "5", "5", "6" ], [ "11", "9", "9", "10" ] ] }
	"""
	def join_when_matched( %{ "columns" => columns, "rows" => rows }, manipulation ) do
#TODO: 列未存在時のエラーログ

#TODO: 同名カラム衝突とリネーム or アベンド＋ログは、追って実装

		#----------------------------------------------
		# phase.0: load join datas
		#----------------------------------------------
#TODO: DBもリードできるように
		join_data = read_from_csv_file( manipulation[ "source" ] )

		#----------------------------------------------
		# phase.1: pickup match input-key/join-key rows
		#----------------------------------------------
		keys = pickup_match_key( columns, join_data[ "columns" ], manipulation[ "options" ] )
#IO.puts "==================================="
#IO.inspect join_data[ "columns" ]
#IO.inspect manipulation[ "options" ]
#IO.puts "==================================="

		join_only_rows = filter_match_row( rows, join_data[ "rows" ], keys )

		#----------------------------------------------
		# phase.2: copy columns
		#----------------------------------------------
		processed_columns = unshift_columns( columns, manipulation[ "destination" ] )

		#----------------------------------------------
		# phase.3: copy column value to rows
		#----------------------------------------------
		join_column_nos = Lst.pickup_match_index( join_data[ "columns" ], manipulation[ "destination" ] )

		processed_rows = unshift_rows( join_only_rows, join_column_nos )

		%{ "columns" => processed_columns, "rows" => processed_rows }
	end

	@doc """
	Join if input exists

	## Examples
		iex> Dataframex.join_if_input_exists( %{ "columns" => [ "c1", "c2" ], "rows" => [ [ "1", "2" ], [ "3", "4" ], [ "5", "6" ] ] }, %{ "source" => "test/dataframe_join.csv", "destination" => [ "j1", "j3" ], "options" => [ "c2", "j2", ] } )
		%{ "columns" => [ "j3", "j1", "c1", "c2" ], "rows" => [ [ "3", "1", "1", "2" ], [ "", "", "3", "4" ], [ "7", "5", "5", "6" ] ] }

		iex> Dataframex.join_if_input_exists( %{ "columns" => [ "c1", "c2" ], "rows" => [ [ "1", "2" ], [ "3", "4" ], [ "5", "6" ], [ "7", "8" ], [ "9", "10" ] ] }, %{ "source" => "test/dataframe_join.csv", "destination" => [ "j1", "j3" ], "options" => [ "c2", "j2", ] } )
		%{ "columns" => [ "j3", "j1", "c1", "c2" ], "rows" => [ [ "3", "1", "1", "2" ], [ "", "", "3", "4" ], [ "7", "5", "5", "6" ], [ "", "", "7", "8" ], [ "11", "9", "9", "10" ],  ] }
	"""
	def join_if_input_exists( %{ "columns" => columns, "rows" => rows }, manipulation ) do
#TODO: 列未存在時のエラーログ

#TODO: 同名カラム衝突とリネーム or アベンド＋ログは、追って実装

		#----------------------------------------------
		# phase.0: load join datas
		#----------------------------------------------
#TODO: DBもリードできるように
		join_data = read_from_csv_file( manipulation[ "source" ] )

		#----------------------------------------------
		# phase.1: pickup input exists input-key/join-key rows
		#----------------------------------------------
		keys = pickup_match_key( columns, join_data[ "columns" ], manipulation[ "options" ] )

		join_only_rows = filter_input_exist_row( rows, join_data[ "rows" ], keys )

		#----------------------------------------------
		# phase.2: copy columns
		#----------------------------------------------
		processed_columns = unshift_columns( columns, manipulation[ "destination" ] )

		#----------------------------------------------
		# phase.3: copy column value to rows
		#----------------------------------------------
		join_column_nos = Lst.pickup_match_index( join_data[ "columns" ], manipulation[ "destination" ] )

		processed_rows = unshift_rows( join_only_rows, join_column_nos )

		%{ "columns" => processed_columns, "rows" => processed_rows }
	end

	@doc """
	Join if join exists

	## Examples
		iex> Dataframex.join_if_join_exists( %{ "columns" => [ "c1", "c2" ], "rows" => [ [ "1", "2" ], [ "3", "4" ], [ "5", "6" ] ] }, %{ "source" => "test/dataframe_join.csv", "destination" => [ "j1", "j3" ], "options" => [ "c2", "j2", ] } )
		%{ "columns" => [ "j3", "j1", "c1", "c2" ], "rows" => [ [ "3", "1", "1", "2" ], [ "7", "5", "5", "6" ], [ "11", "9", "", "" ], [ "15", "13", "", "" ] ] }

		iex> Dataframex.join_if_join_exists( %{ "columns" => [ "c1", "c2" ], "rows" => [ [ "1", "2" ], [ "3", "4" ], [ "5", "6" ], [ "7", "8" ], [ "9", "10" ] ] }, %{ "source" => "test/dataframe_join.csv", "destination" => [ "j1", "j3" ], "options" => [ "c2", "j2", ] } )
		%{ "columns" => [ "j3", "j1", "c1", "c2" ], "rows" => [ [ "3", "1", "1", "2" ], [ "7", "5", "5", "6" ], [ "11", "9", "9", "10" ], [ "15", "13", "", "" ] ] }
	"""
	def join_if_join_exists( %{ "columns" => columns, "rows" => rows }, manipulation ) do
#TODO: 列未存在時のエラーログ

#TODO: 同名カラム衝突とリネーム or アベンド＋ログは、追って実装

		#----------------------------------------------
		# phase.0: load join datas
		#----------------------------------------------
#TODO: DBもリードできるように
		join_data = read_from_csv_file( manipulation[ "source" ] )

		#----------------------------------------------
		# phase.1: pickup join exists input-key/join-key rows
		#----------------------------------------------
		keys = pickup_match_key( columns, join_data[ "columns" ], manipulation[ "options" ] )

		join_only_rows = filter_join_exist_row( rows, join_data[ "rows" ], keys )

		#----------------------------------------------
		# phase.2: copy columns
		#----------------------------------------------
		processed_columns = unshift_columns( columns, manipulation[ "destination" ] )

		#----------------------------------------------
		# phase.3: copy column value to rows
		#----------------------------------------------
		join_column_nos = Lst.pickup_match_index( join_data[ "columns" ], manipulation[ "destination" ] )

		processed_rows = unshift_rows( join_only_rows, join_column_nos )

		%{ "columns" => processed_columns, "rows" => processed_rows }
	end

	#-----------------------------------------------------------------------
	#	Row
	#-----------------------------------------------------------------------

	@doc """
	Drop row

	## Examples
		iex>
		nil
	"""
	def drop_row( %{ "columns" => columns, "rows" => rows }, manipulation ) do
		[ column_no ] = Lst.pickup_match_index( columns, [ manipulation[ "source" ] ] )
		if column_no == nil do
			#TODO: エラーログ
			message = "not exists column reference '" <> inspect( manipulation[ "source" ] ) <> "'(" <> inspect( column_no ) <> ")"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts message
			IO.puts "-------------------------------------------------------"
			IO.puts "(but here is through and execution continue)"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

			%{ "columns" => columns, "rows" => rows, "error" => message }
		else
			processed_rows = rows
				|> Enum.map( fn row ->

#TODO: タイプ判定が無い

#TODO: 「==」と「!=」以外は対応していないので注意（演算子と判定は、もーちょい頭良くしたい）
#TODO: 正規表現と上記をスイッチできるように
#TODO: extract_columnとセットで改修
					cond do
						manipulation[ "destination" ] |> String.contains?( "==" ) ->
							condition = String.trim( String.trim( String.replace( manipulation[ "destination" ], "==", "" ) ), "\"" )
							if ( Enum.at( row, column_no ) == condition ) == false do
								row
							end
						manipulation[ "destination" ] |> String.contains?( "!=" ) ->
							condition = String.trim( String.trim( String.replace( manipulation[ "destination" ], "!=", "" ) ), "\"" )
							if ( Enum.at( row, column_no ) != condition ) == false do
								row
							end
						true ->
							IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
							IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
							IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
							IO.puts "unsupport operation"
							IO.puts manipulation[ "destination" ]
							IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
							IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
							IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
							nil
					end
				end )
				|> Enum.filter( & &1 != nil )

			%{ "columns" => columns, "rows" => processed_rows }
		end
	end

	@doc """
	Sort row

	## Examples
		iex> Dataframex.sort_row(%{ "columns" => [ "c1", "c2" ], "rows" => [ [ "1", "2" ], [ "6", "7" ], [ "11", "12" ] ] }, %{ "source" => "c1" } )
		%{ "columns" => ["c1", "c2"], "rows" => [ [ "1", "2" ], [ "11", "12" ], [ "6", "7" ] ] }
		iex> Dataframex.sort_row(%{ "columns" => [ "c1", "c2" ], "rows" => [ [ "1", "2" ], [ "6", "7" ], [ "11", "12" ] ] }, %{ "source" => "c2" } )
		%{ "columns" => ["c1", "c2"], "rows" => [ [  "11", "12" ], [ "1", "2" ], [ "6", "7" ] ] }
	"""
	def sort_row( %{ "columns" => columns, "rows" => rows }, manipulation ) do
		[ column_no ] = Lst.pickup_match_index( columns, [ manipulation[ "source" ] ] )
		if column_no == nil do
			#TODO: エラーログ
			message = "not exists column reference '" <> inspect( manipulation[ "source" ] ) <> "'(" <> inspect( column_no ) <> ")"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts message
			IO.puts "-------------------------------------------------------"
			IO.puts "(but here is through and execution continue)"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

			%{ "columns" => columns, "rows" => rows, "error" => message }
		else
#TODO: 第二優先や降順も忘れずに
			processed_rows = rows
				|> Enum.sort( fn row1, row2 -> Enum.at( row1, column_no ) < Enum.at( row2, column_no ) end )

			%{ "columns" => columns, "rows" => processed_rows }
		end
	end

	@doc """
	Unique row

	## Examples
		iex> Dataframex.unique_row( %{ "columns" => [ "c1", "c2" ], "rows" => [ [ "1", "2" ], [ "1", "4" ], [ "5", "6" ] ] }, %{ "source" => "c1", "destination" => "c2", "options" => [ "max" ] } )
		%{ "columns" => [ "c1", "c2" ], "rows" => [ [ "1", "4" ], [ "5", "6" ] ] }
		iex> Dataframex.unique_row( %{ "columns" => [ "c1", "c2", "c3" ], "rows" => [ [ "1", "2", "3" ], [ "1", "4", "4" ], [ "1", "5", "6" ] ] }, %{ "source" => "c1", "destination" => "c2", "options" => [ "max" ] } )
		%{ "columns" => [ "c1", "c2", "c3" ], "rows" => [ [ "1", "5", "6" ] ] }
		iex> Dataframex.unique_row( %{ "columns" => [ "c1", "c2", "c3" ], "rows" => [ [ "1", "", "3" ], [ "1", "", "4" ], [ "1", "", "6" ] ] }, %{ "source" => "c2", "destination" => "c3", "options" => [ "max" ] } )
		%{ "columns" => [ "c1", "c2", "c3" ], "rows" => [ [ "1", "", "6" ] ] }
		iex> Dataframex.unique_row( %{ "columns" => [ "c1", "c2", "c3" ], "rows" => [ [ "1", "", "3" ], [ "1", "", "4" ], [ "1", "", "6" ] ] }, %{ "source" => "c2", "destination" => "c1", "options" => [ "max" ] } )
		%{ "columns" => [ "c1", "c2", "c3" ], "rows" => [ [ "1", "", "3" ] ] }
	"""
	def unique_row( %{ "columns" => columns, "rows" => rows }, manipulation ) do
		[ unique_column_no ] = Lst.pickup_match_index( columns, [ manipulation[ "source" ] ] )
		[ pickup_column_no ] = Lst.pickup_match_index( columns, [ manipulation[ "destination" ] ] )
		if unique_column_no == nil || pickup_column_no == nil do
			#TODO: エラーログ（2列）
			message = "not exists column reference '" <> inspect( manipulation[ "source" ] ) <> "'(" <> inspect( unique_column_no ) <> "), " <> inspect( manipulation[ "destination" ] ) <> "'(" <> inspect( pickup_column_no ) <> ")"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts message
			IO.puts "-------------------------------------------------------"
			IO.puts "(but here is through and execution continue)"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

			%{ "columns" => columns, "rows" => rows, "error" => message }
		else
			uniques = rows
				|> Enum.map( fn row -> [ Enum.at( row, unique_column_no ), Enum.at( row, pickup_column_no ) ] end )
				|> Enum.group_by( fn row -> Enum.at( row, 0 ) end, fn row -> Enum.at( row, 1 ) end )
				|> Enum.map( fn row ->
#TODO: リストでは無くマップの方がいい？、↓
					[
						elem( row, 0 ),
#TODO: mix、maxなどを網羅すること
						case Enum.at( manipulation[ "options" ], 0 ) do
							"min"	-> elem( row, 1 ) |> Enum.min
							_		-> elem( row, 1 ) |> Enum.max
						end
					]
				end )

			filtered_rows = rows
				|> Enum.filter( fn row ->
					uniques |> Enum.map( fn unique ->
						Enum.at( row, unique_column_no ) == Enum.at( unique, 0 ) && Enum.at( row, pickup_column_no ) == Enum.at( unique, 1 )
					end )
					|> Enum.find( & &1 == true ) != nil
				end )

			processed_rows = filtered_rows
				|> Enum.uniq_by( fn row ->
					uniques |> Enum.map( fn unique ->
						Enum.at( row, unique_column_no ) == Enum.at( unique, 0 )
					end )
				end )

			%{ "columns" => columns, "rows" => processed_rows }
		end
	end

	#-----------------------------------------------------------------------
	#	Column value
	#-----------------------------------------------------------------------

	@doc """
	Map value

	## Examples
		iex>
		nil
	"""
	def map_value( %{ "columns" => columns, "rows" => rows }, manipulation ) do
		[ column_no ] = Lst.pickup_match_index( columns, [ manipulation[ "source" ] ] )
		if column_no == nil do
			#TODO: エラーログ
			message = "not exists column reference '" <> inspect( manipulation[ "source" ] ) <> "'(" <> inspect( column_no ) <> ")"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts message
			IO.puts "-------------------------------------------------------"
			IO.puts "(but here is through and execution continue)"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

			%{ "columns" => columns, "rows" => rows, "error" => message }
		else
			maps = read_map_file( "./esuna_maps/" <> manipulation[ "destination" ] )
			originals = maps |> Enum.map( & &1 |> List.first )

			processed_rows = rows
				|> Enum.map( fn row ->
					map_no = Enum.find_index( originals, & &1 == Enum.at( row, column_no ) )
					if map_no != nil do
						List.replace_at( row, column_no, Enum.at( Enum.at( maps, map_no ), 1 ) )
					else
						row
					end
				end )

			%{ "columns" => columns, "rows" => processed_rows }
		end
	end

	@doc """
	Replace value

	## Examples
		iex>
		nil
	"""
	def replace_value( %{ "columns" => columns, "rows" => rows }, manipulation ) do
		[ column_no ] = Lst.pickup_match_index( columns, [ manipulation[ "source" ] ] )
		if column_no == nil do
			#TODO: エラーログ
			message = "not exists column reference '" <> inspect( manipulation[ "source" ] ) <> "'(" <> inspect( column_no ) <> ")"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts message
			IO.puts "-------------------------------------------------------"
			IO.puts "(but here is through and execution continue)"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

			%{ "columns" => columns, "rows" => rows, "error" => message }
		else
			processed_rows = rows
				|> Enum.map( fn row ->
					value = Enum.at( row, column_no )
					pattern = Regex.compile!( manipulation[ "destination" ] )
					if Regex.match?( pattern, value ) do
						List.replace_at( row , column_no, Enum.at( manipulation[ "options" ], 0 ) )
					else
						row
					end
				end )

			%{ "columns" => columns, "rows" => processed_rows }
		end
	end

	@doc """
	Replace other column value

	## Examples
		iex>
		nil
	"""
	def replace_other_column_value( %{ "columns" => columns, "rows" => rows }, manipulation ) do
		[ column_no ]       = Lst.pickup_match_index( columns, [ manipulation[ "source" ] ] )
		[ other_column_no ] = Lst.pickup_match_index( columns, [ Enum.at( manipulation[ "options" ], 0 ) ] )
		if column_no == nil || other_column_no == nil do
			#TODO: エラーログ
			message = "not exists column reference '" <> inspect( manipulation[ "source" ] ) <> "'(" <> inspect( column_no ) <> ")"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts message
			IO.puts "-------------------------------------------------------"
			IO.puts "(but here is through and execution continue)"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

			%{ "columns" => columns, "rows" => rows, "error" => message }
		else
			processed_rows = rows
				|> Enum.map( fn row ->
					value = Enum.at( row, column_no )
					pattern = Regex.compile!( manipulation[ "destination" ] )
					if Regex.match?( pattern, value ) do
						List.replace_at( row , other_column_no, Enum.at( manipulation[ "options" ], 1 ) )
					else
						row
					end
				end )

			%{ "columns" => columns, "rows" => processed_rows }
		end
	end

	@doc """
	Retrieve value

	## Examples
		iex>
		nil
	"""
	def retrieve_value( %{ "columns" => columns, "rows" => rows }, manipulation ) do
		[ column_no ] = Lst.pickup_match_index( columns, [ manipulation[ "source" ] ] )
		if column_no == nil do
			#TODO: エラーログ
			message = "not exists column reference '" <> inspect( manipulation[ "source" ] ) <> "'(" <> inspect( column_no ) <> ")"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts message
			IO.puts "-------------------------------------------------------"
			IO.puts "(but here is through and execution continue)"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

			%{ "columns" => columns, "rows" => rows, "error" => message }
		else
			processed_rows = rows
				|> Enum.map( fn row ->
					value = Enum.at( row, column_no )
					pattern = Regex.compile!( manipulation[ "destination" ] )
					capture_map = Regex.named_captures( pattern, value )
					if capture_map[ "value" ] != nil do
						List.replace_at( row , column_no, capture_map[ "value" ] )
					else
						List.replace_at( row , column_no, "" )
					end
				end )

			%{ "columns" => columns, "rows" => processed_rows }
		end
	end

	@doc """
	Calculate value

	## Examples
		iex>
		nil
	"""
	def calculate_value( %{ "columns" => columns, "rows" => rows }, manipulation ) do
		[ column_no ] = Lst.pickup_match_index( columns, [ manipulation[ "source" ] ] )
		if column_no == nil do
			#TODO: エラーログ
			message = "not exists column reference '" <> inspect( manipulation[ "source" ] ) <> "'(" <> inspect( column_no ) <> ")"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts message
			IO.puts "-------------------------------------------------------"
			IO.puts "(but here is through and execution continue)"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

			%{ "columns" => columns, "rows" => rows, "error" => message }
		else
			processed_rows = rows
				|> Enum.map( fn row ->
					value  = Type.to_number( Enum.at( row, column_no ) )
					effect = Type.to_number( Enum.at( manipulation[ "options" ], 0 ) )
					calculated = case manipulation[ "destination" ] do
						"+"	-> value + effect
						"-"	-> value - effect
						"*"	-> value * effect
						"/"	-> value / effect
						_	-> value
					end
					List.replace_at( row , column_no, Type.to_string( calculated ) )
				end )

			%{ "columns" => columns, "rows" => processed_rows }
		end
	end

	@doc """
	Fill missing

	## Examples
		iex>
		nil
	"""
	def fill_missing( %{ "columns" => columns, "rows" => rows }, manipulation ) do
		[ column_no ] = Lst.pickup_match_index( columns, [ manipulation[ "source" ] ] )
		if column_no == nil do
			#TODO: エラーログ
			message = "not exists column reference '" <> inspect( manipulation[ "source" ] ) <> "'(" <> inspect( column_no ) <> ")"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts message
			IO.puts "-------------------------------------------------------"
			IO.puts "(but here is through and execution continue)"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

			%{ "columns" => columns, "rows" => rows, "error" => message }
		else
			processed_rows = rows
				|> Enum.map( fn row ->
					value = Enum.at( row, column_no )
					if value == nil || value == "" do
						List.replace_at( row , column_no, manipulation[ "destination" ] )
					else
						row
					end
				end )

			%{ "columns" => columns, "rows" => processed_rows }
		end
	end

	@doc """
	One Hot Encoding

	## Examples
		iex>
		nil
	"""
	def one_hot_encoding( %{ "columns" => columns, "rows" => rows }, manipulation ) do
		[ column_no ] = Lst.pickup_match_index( columns, [ manipulation[ "source" ] ] )
		if column_no == nil do
			#TODO: エラーログ
			message = "not exists column reference '" <> inspect( manipulation[ "source" ] ) <> "'(" <> inspect( column_no ) <> ")"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts message
			IO.puts "-------------------------------------------------------"
			IO.puts "(but here is through and execution continue)"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

			%{ "columns" => columns, "rows" => rows, "error" => message }
		else
#TODO: 実装
#TODO: 実装
#TODO: 実装
			processed_rows = rows
			%{ "columns" => columns, "rows" => processed_rows }
		end
	end

	@doc """
	Group by

	## Examples
		iex>
		nil
	"""
	def group_by( %{ "columns" => columns, "rows" => rows }, manipulation ) do
		[ column_no ] = Lst.pickup_match_index( columns, [ manipulation[ "source" ] ] )
		if column_no == nil do
			#TODO: エラーログ
			message = "not exists column reference '" <> inspect( manipulation[ "source" ] ) <> "'(" <> inspect( column_no ) <> ")"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts message
			IO.puts "-------------------------------------------------------"
			IO.puts "(but here is through and execution continue)"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

			%{ "columns" => columns, "rows" => rows, "error" => message }
		else
#TODO: 実装
#TODO: 実装
#TODO: 実装
			processed_rows = rows
			%{ "columns" => columns, "rows" => processed_rows }
		end
	end

	#-----------------------------------------------------------------------
	#	Column
	#-----------------------------------------------------------------------

	@doc """
	Rename columns

	## Examples
		iex> Dataframex.rename_column( %{ "columns" => [ "c1", "c2", "c3", "c4" ], "rows" => [ [ 1, 2, 3, 4 ], [ 6, 7, 8, 9 ] ] }, [ %{ "source" => "c2", "destination" => "l2" } ] )
		%{ "columns" => [ "c1", "l2", "c3", "c4" ], "rows" => [ [ 1, 2, 3, 4 ], [ 6, 7, 8, 9 ] ] }
	"""
	def rename_column( %{ "columns" => columns, "rows" => rows }, manipulations ) do
		if manipulations != [] do
			sources      = manipulations |> Enum.map( & &1[ "source" ] )
			destinations = manipulations |> Enum.map( & &1[ "destination" ] )
			processed_columns = columns
				|> Enum.map( fn column_name ->
					index = Enum.find_index( sources, & &1 == column_name )
					if index != nil, do: Enum.at( destinations, index ), else: column_name
				end )

			%{ "columns" => processed_columns, "rows" => rows }
		else
			%{ "columns" => columns, "rows" => rows }
		end
	end

	@doc """
	Drop columns in rows

	## Examples
		iex> Dataframex.drop_column( %{ "columns" => [ "c1", "c2", "c3", "c4", "c5" ], "rows" => [ [ 1, 2, 3, 4, 5 ], [ 6, 7, 8, 9, 0 ] ] }, [ %{ "source" => "c2", "dummy" => 21 }, %{ "source" => "c4", "dummy" => 42 } ] )
		%{ "columns" => [ "c1", "c3", "c5" ], "rows" => [ [ 1, 3, 5 ], [ 6, 8, 0 ] ] }
	"""
	def drop_column( %{ "columns" => columns, "rows" => rows }, manipulations ) do
		if manipulations != [] do
			names = Lst.list_from_map( manipulations, "source" )
#TODO: 列未存在時のエラーログ

			processed_columns = Lst.pickup_unmatch( columns, names )

			processed_rows =
				rows
				|> Enum.map( & List.zip( [ Lst.to_atoms_from_strings( columns ), &1 ] ) )
				|> Enum.map( & Lst.delete_by_keys( &1, names ) )
				|> Enum.map( & Enum.unzip( &1 ) |> elem( 1 ) )

			%{ "columns" => processed_columns, "rows" => processed_rows }
		else
			%{ "columns" => columns, "rows" => rows }
		end
	end

	@doc """
	Drop columns in rows

	## Examples
		iex> Dataframex.drop_column_flow( %{ "columns" => [ "c1", "c2", "c3", "c4", "c5" ], "rows" => [ [ 1, 2, 3, 4, 5 ], [ 6, 7, 8, 9, 0 ] ] }, [ %{ "source" => "c2", "dummy" => 21 }, %{ "source" => "c4", "dummy" => 42 } ] )
		%{ "columns" => [ "c1", "c3", "c5" ], "rows" => [ [ 1, 3, 5 ], [ 6, 8, 0 ] ] }
	"""
	def drop_column_flow( %{ "columns" => columns, "rows" => rows }, manipulations ) do
		if manipulations != [] do
			names = Lst.list_from_map( manipulations, "source" )
#TODO: 列未存在時のエラーログ

			processed_columns = Lst.pickup_unmatch( columns, names )

			processed_rows =
				rows
				|> Flow.from_enumerable()
				|> Flow.map( & List.zip( [ Lst.to_atoms_from_strings( columns ), &1 ] ) )
				|> Flow.map( & Lst.delete_by_keys( &1, names ) )
				|> Enum.map( & Enum.unzip( &1 ) |> elem( 1 ) )

			%{ "columns" => processed_columns, "rows" => processed_rows }
		else
			%{ "columns" => columns, "rows" => rows }
		end
	end

	@doc """
	Add columns

	## Examples
		iex> Dataframex.add_column( %{ "columns" => [ "c1", "c2" ], "rows" => [ [ 1, 2 ], [ 6, 7 ], [ 11, 12 ] ] }, [ %{ "destination" => "c3", "options" => [ 0 ] } ] )
		%{ "columns" => [ "c3", "c1", "c2" ], "rows" => [ [ 0, 1, 2 ], [ 0, 6, 7 ], [ 0, 11, 12 ] ] }

		#iex> Dataframex.add_column( %{ "columns" => [ "c1", "c2" ], "rows" => [ [ 1, 2 ], [ 6, 7 ], [ 11, 12 ] ] }, [ %{ "destination" => "c3", "options" => [ 0 ] }, %{ "destination" => "c4", "options" => [ 9 ] } ] )
		#%{ "columns" => [ "c3", "c4", "c1", "c2" ], "rows" => [ [ 0, 9, 1, 2 ], [ 0, 9, 6, 7 ], [ 0, 9, 11, 12 ] ] }
	"""
	def add_column( %{ "columns" => columns, "rows" => rows }, manipulations ) do
		if manipulations != [] do
			add_columns	= Lst.list_from_map( manipulations, "destination" )
			add_values	= Lst.list_from_map( manipulations, "options", 0 )

			processed_columns = add_columns ++ columns

#TODO: 複数列に対応する
			values = Stream.iterate( List.first( add_values ), &( &1 ) ) |> Enum.take( Enum.count( rows ) )

			processed_rows = Lst.merge( rows, values )

			%{ "columns" => processed_columns, "rows" => processed_rows }
		else
			%{ "columns" => columns, "rows" => rows }
		end
	end

	@doc """
	Add auto number columns

	## Examples
		iex> Dataframex.add_auto_number_column( %{ "columns" => [ "c1", "c2" ], "rows" => [ [ 1, 2 ], [ 6, 7 ], [ 11, 12 ] ] }, [ %{ "destination" => "c3", "options" => [ 0, 1 ] } ] )
		%{ "columns" => [ "c3", "c1", "c2" ], "rows" => [ [ 0, 1, 2 ], [ 1, 6, 7 ], [ 2, 11, 12 ] ] }

		#iex> Dataframex.add( %{ "columns" => [ "c1", "c2" ], "rows" => [ [ 1, 2 ], [ 6, 7 ], [ 11, 12 ] ] }, [ %{ "destination" => "c3", "options" => [ 0, 1 ] }, %{ "destination" => "c4", "options" => [ 2, 3 ] } ] )
		#%{ "columns" => [ "c3", "c4", "c1", "c2" ], "rows" => [ [ 0, 2, 1, 2 ], [ 1, 5, 6, 7 ], [ 2, 8, 11, 12 ] ] }
	"""
	def add_auto_number_column( %{ "columns" => columns, "rows" => rows }, manipulations, before_rows \\ 0 ) do
		if manipulations != [] do
			add_columns	= Lst.list_from_map( manipulations, "destination" )
			starts		= Lst.list_from_map( manipulations, "options", 0 )
			steps		= Lst.list_from_map( manipulations, "options", 1 )

			processed_columns = add_columns ++ columns

#TODO: 複数列に対応する
			values = Stream.iterate( List.first( starts ) + before_rows, &( &1 + List.first( steps ) ) )
				|> Enum.take( Enum.count( rows ) )

			processed_rows = Lst.merge( rows, values )

			%{ "columns" => processed_columns, "rows" => processed_rows }
		else
			%{ "columns" => columns, "rows" => rows }
		end
	end

	@doc """
	Duplicate column

	## Examples
		iex>
		nil
	"""
	def duplicate_column( %{ "columns" => columns, "rows" => rows }, manipulation ) do
		[ column_no ] = Lst.pickup_match_index( columns, [ manipulation[ "source" ] ] )
		if column_no == nil do
			#TODO: エラーログ
			message = "not exists column reference '" <> inspect( manipulation[ "source" ] ) <> "'(" <> inspect( column_no ) <> ")"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts message
			IO.puts "-------------------------------------------------------"
			IO.puts "(but here is through and execution continue)"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

			%{ "columns" => columns, "rows" => rows, "error" => message }
		else
			processed_columns = [ manipulation[ "destination" ] ] ++ columns

			values = rows
				|> Enum.map( fn row -> Enum.at( row, column_no ) end )

			processed_rows = Lst.merge( rows, values )

			%{ "columns" => processed_columns, "rows" => processed_rows }
		end
	end

	@doc """
	Extract column

	## Examples
		iex>
		nil
	"""
	def extract_column( %{ "columns" => columns, "rows" => rows }, manipulation ) do
		[ column_no ]         = Lst.pickup_match_index( columns, [ manipulation[ "source" ] ] )
		[ extract_column_no ] = Lst.pickup_match_index( columns, [ Enum.at( manipulation[ "options" ], 0 ) ] )
		if column_no == nil || extract_column_no == nil do
			#TODO: エラーログ
			message = "not exists column reference '" <> inspect( manipulation[ "source" ] ) <> "'(" <> inspect( column_no ) <> ")"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts message
			IO.puts "-------------------------------------------------------"
			IO.puts "(but here is through and execution continue)"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

			%{ "columns" => columns, "rows" => rows, "error" => message }
		else
			processed_columns = [ manipulation[ "destination" ] ] ++ columns

			processed_rows = rows
				|> Enum.map( fn row ->

#TODO: タイプ判定が無い

#TODO: 「==」と「!=」以外は対応していないので注意（演算子と判定は、もーちょい頭良くしたい）
#TODO: 正規表現と上記をスイッチできるように
#TODO: drop_rowとセットで改修
					pattern = Enum.at( manipulation[ "options" ], 1 )
					cond do
						pattern |> String.contains?( "==" ) ->
							condition = String.trim( String.trim( String.replace( pattern, "==", "" ) ), "\"" )
							if ( Enum.at( row, extract_column_no ) == condition ) == true do
								[ Enum.at( row, column_no ) ] ++ row
							else
								[ "" ] ++ row
							end
						pattern |> String.contains?( "!=" ) ->
							condition = String.trim( String.trim( String.replace( pattern, "!=", "" ) ), "\"" )
							if ( Enum.at( row, extract_column_no ) != condition ) == true do
								[ Enum.at( row, column_no ) ] ++ row
							else
								[ "" ] ++ row
							end
						true ->
							IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
							IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
							IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
							IO.puts "unsupport operation"
							IO.puts pattern
							IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
							IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
							IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
							nil
					end
				end )



			%{ "columns" => processed_columns, "rows" => processed_rows }
		end
	end

	@doc """
	Combine columns

	## Examples
		iex>
		nil
	"""
	def combine_columns( %{ "columns" => columns, "rows" => rows }, manipulation ) do
		[ column_no ]      = Lst.pickup_match_index( columns, [ manipulation[ "source" ] ] )
		[ with_column_no ] = Lst.pickup_match_index( columns, [ Enum.at( manipulation[ "options" ], 0 ) ] )
		if column_no == nil || with_column_no == nil do
			#TODO: エラーログ
			message = "not exists column reference '" <> inspect( manipulation[ "source" ] ) <> "'(" <> inspect( column_no ) <> ")"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts message
			IO.puts "-------------------------------------------------------"
			IO.puts "(but here is through and execution continue)"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

			%{ "columns" => columns, "rows" => rows, "error" => message }
		else
			processed_columns = [ manipulation[ "destination" ] ] ++ columns

			values = rows
				|> Enum.map( fn row -> Enum.at( row, column_no ) <> Enum.at( row, with_column_no ) end )

			processed_rows = Lst.merge( rows, values )

			%{ "columns" => processed_columns, "rows" => processed_rows }
		end
	end

	@doc """
	Pickup column

	## Examples
		iex>
		nil
	"""
	def pickup_column( %{ "columns" => columns, "rows" => rows }, manipulation ) do
		[ column_no ] = Lst.pickup_match_index( columns, [ manipulation[ "source" ] ] )
		if column_no == nil do
			#TODO: エラーログ
			message = "not exists column reference '" <> inspect( manipulation[ "source" ] ) <> "'(" <> inspect( column_no ) <> ")"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts message
			IO.puts "-------------------------------------------------------"
			IO.puts "(but here is through and execution continue)"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			IO.puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

			%{ "columns" => columns, "rows" => rows, "error" => message }
		else
			processed_columns = [ Enum.at( columns, column_no ) ]

			processed_rows = rows
				|> Enum.map( fn row -> [ Enum.at( row, column_no ) ] end )

			%{ "columns" => processed_columns, "rows" => processed_rows }
		end
	end

	@doc """
	Write dataframe to csv file

	## Examples
		#iex> File.rm!( "test/dataframe.csv" )
		#iex> Dataframex.write_to_csv_file( %{ "columns" => [ "c3","c4","c1","c2" ], "rows" => [ [ 0, 9, 1, 2 ], [ 0, 9, 6, 7 ], [ 0, 9, 11, 12 ] ] }, "test/dataframe.csv" )
		#%{ "columns" => [ "c3","c4","c1","c2" ], "rows" => [ [ 0, 9, 1, 2 ], [ 0, 9, 6, 7 ], [ 0, 9, 11, 12 ] ] }
		#iex> File.rm!( "test/dataframe.csv" )
		#:ok
	"""
	def write_to_csv_file( %{ "columns" => columns, "rows" => rows } = dataframe, path ) do
		body =
			Lst.to_csv( columns, [ quote: "\"", ] ) <> "\n"
			<>
			Enum.reduce( rows, "", fn row, acc -> acc <> Lst.to_csv( row, [ quote: "\"" ] ) <> "\n" end )

		File.write!( path, body )

		dataframe
	end

	@doc """
	Read dataframe from csv file

	## Examples
		iex> Dataframex.write_to_csv_file( %{ "columns" => [ "c3","c4","c1","c2" ], "rows" => [ [ 0, 9, 1, 2 ], [ 0, 9, 6, 7 ], [ 0, 9, 11, 12 ] ] }, "test/dataframe.csv" )
		iex> Dataframex.read_from_csv_file( "test/dataframe.csv" )
		%{ "columns" => [ "c3","c4","c1","c2" ], "rows" => [ [ "0","9","1","2" ], [ "0","9","6","7" ], [ "0","9","11","12" ] ] }
		#iex> File.rm!( "test/dataframe.csv" )
		#:ok
	"""
	def read_from_csv_file( path, head \\ -1 ) do
		path
		|> File.stream!
		|> CSV.decode
		|> Enum.take( if head == -1, do: 9999999999999999, else: head )
		|> Stream.filter( &( elem( &1, 0 ) == :ok ) )	#TODO: apply libraried
		|> Stream.map( & elem( &1, 1 )
			|> Enum.map( fn column -> column
				|> String.replace( "\"", "\"\"" )
				|> String.replace( "\n\r\n", "\n" )
			end )
		)
		|> Enum.to_list
		|> Lst.separate( "columns", "rows" ) #TODO: smallexリネーム（初見で分からない）
	end

	@doc """
	Read dataframe from csv file

	## Examples
		iex> Dataframex.write_to_csv_file( %{ "columns" => [ "c3","c4","c1","c2" ], "rows" => [ [ 0, 9, 1, 2 ], [ 0, 9, 6, 7 ], [ 0, 9, 11, 12 ] ] }, "test/dataframe.csv" )
		iex> Dataframex.read_from_csv_file( "test/dataframe.csv" )
		%{ "columns" => [ "c3","c4","c1","c2" ], "rows" => [ [ "0","9","1","2" ], [ "0","9","6","7" ], [ "0","9","11","12" ] ] }
		#iex> File.rm!( "test/dataframe.csv" )
		#:ok
	"""
	def read_map_file( path ) do
		path
		|> File.stream!
		|> CSV.decode
		|> Enum.filter( &( elem( &1, 0 ) == :ok ) ) #TODO: Tpl？ライブラリ化 Tpl.ok
		|> Enum.map( &( elem( &1, 1 ) ) )
	end

#TODO: 実装
#TODO: 実装
#TODO: 実装
	@doc """
	Add type and statistics to dataframe

	## Examples
		#iex> Dataframex.add_type_and_statistics( %{ "columns" => [ "c3","c4","c1","c2" ], "rows" => [ [ 0, 9, 1, 2 ], [ 0, 9, 6, 7 ], [ 0, 9, 11, 12 ] ] } )
		#%{ "columns" => [ "c3","c4","c1","c2" ], "rows" => [ [ "0","9","1","2" ], [ "0","9","6","7" ], [ "0","9","11","12" ] ] }
	"""
	def add_type_and_statistics( dataframe ) do
		columns = dataframe[ "columns" ]
			|> Enum.map( &
				%{
					"name"			=> &1,
					"type"			=> "string", #TODO
					"division"		=>
						%{ "valid"	=> "70%", "mismatch" => "23%", "missing" => "7%" }, #TODO
					"distributions"	=>
						[
							%{ "label" => "0 - 50",    "value" => 49, "ratio" => "5.50%" }, #TODO
							%{ "label" => "51 - 100",  "value" => 50, "ratio" => "4.71%" },
							%{ "label" => "101 - 150", "value" => 50, "ratio" => "4.71%" },
							%{ "label" => "151 - 200", "value" => 49, "ratio" => "5.50%" },
							%{ "label" => "201 - 250", "value" => 50, "ratio" => "4.71%" },
						],
					"summaries"		=> "-", #TODO
				} )

		%{
			"columns"	=> columns,
			"rows"		=> dataframe[ "rows" ],
		}
	end

	@doc """
	Read DB

	## Examples
		iex>
		nil
	"""
	def read_db( table, head \\ -1 ) do
		records = DB.execute( "select * from #{ table }" )

		%{
			"columns"	=> records[ "columns" ],
			"rows"		=> records[ "rows" ] |> Enum.take( if head == -1, do: 9999999999999999, else: head ) |> Enum.map( fn row -> row |> Enum.map( fn column -> Type.to_string_datetime( column ) end ) end )
		}
	end

	@doc """
	Write DB

	## Examples
		iex>
		nil
	"""
	def write_db( %{ "columns" => columns, "rows" => rows }, table ) do
		query = "insert into " <> table <> " ( " <> Lst.to_csv( columns ) <> " ) values( "

		rows
		|> Enum.map( fn row ->
			#TODO: エラーハンドリング向けにどう実装するか？
			#IO.puts "\n"
			row_escaped = row
				|> Enum.map( fn column ->
					#TODO: エラーハンドリング向けにどう実装するか？
					#IO.inspect column
					column |> String.replace( "'", "" )
				end )
			query <> ( Lst.to_csv( row_escaped, [ quote: "'" ] ) |> String.replace( "''", "null" ) ) <> " )" |> DB.execute
		end )
	end

end
