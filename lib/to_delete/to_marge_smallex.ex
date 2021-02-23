defmodule Lst do
  @moduledoc """
  List library.
  """

	@doc """
	To CSV
​
	## Examples
		iex> Lst.to_csv( [ 1, "ab", 8, true ] )
		"1,ab,8,true"
		# iex> Lst.to_csv( [ 1, "ab", 8, true ], :quote )
		# "\"1\",\"ab\",\"8\",\"true\""
​
		iex> Lst.to_csv( [ 1, "ab", 8, true ], [ quote: "'" ] )
		"'1','ab','8','true'"
		# iex> Lst.to_csv( [ 1, "ab", 8, true ], [ quote: "\"" ] )
		# "\"1\",\"ab\",\"8\",\"true\""
​
		iex> Lst.to_csv( [ 1, "ab", 8, true ], [ separator: ", ", post_separator: "-- " ] )
		"1, -- ab, -- 8, -- true"
		iex> Lst.to_csv( [ 1, "ab", 8, true ], [ quote: "'", separator: ", ", post_separator: "-- " ] )
		"'1', -- 'ab', -- '8', -- 'true'"
		iex> Lst.to_csv( [ 1, "ab", 8, true ], [ quote: "'", separator: " / ", post_separator: "- " ] )
		"'1' / - 'ab' / - '8' / - 'true'"
	"""
	def to_csv( list, options \\ nil ) do
		quote =
			if is_list( options ) && options[ :quote ] != nil do
				options[ :quote ]
			else
				if options == :quote do
					"\""
				else
					""
				end
			end
		separator      = if is_list( options ) && options[ :separator ]      != nil, do: options[ :separator ],      else: ","
		post_separator = if is_list( options ) && options[ :post_separator ] != nil, do: options[ :post_separator ], else: ""

		list
		|> Enum.reduce( "", fn v, acc -> "#{ acc }#{ separator }#{ post_separator }#{ quote }#{ v }#{ quote }" end )
		|> String.slice( String.length( "#{ separator }#{ post_separator }" )..-1 )
	end

	@doc """
	Calculate frequency of values ftom list

	## Examples
		iex> Lst.frequency( [ "abc", "abc", "xyz", "abc", "def", "xyz" ] )
		%{ "abc" => 3, "def" => 1, "xyz" => 2 }
		iex> Lst.frequency( [ %{ "a" => "abc" }, %{ "a" => "abc" }, %{ "a" => "xyz" }, %{ "a" => "abc" }, %{ "a" => "def" }, %{ "a" => "xyz" } ] )
		%{ %{ "a" => "abc"} => 3, %{ "a" => "def" } => 1, %{ "a" => "xyz" } => 2 }
	"""
	def frequency( list ), do: list |> Enum.reduce( %{}, fn( k, acc ) -> Map.update( acc, k, 1, &( &1 + 1 ) ) end )

	@doc """
	Zip two lists to map

	## Examples
		iex> Lst.zip( [ "a", "b", "c" ], [ 1, 2, 3 ] )
		%{ "a" => 1, "b" => 2, "c" => 3 }
		iex> Lst.zip( [ "a", "b", "c" ], [ 1, 2, 3 ], :atom )
		%{ a: 1, b: 2, c: 3 }
	"""
	def zip( list1, list2, :atom ),    do: Enum.zip( list1, list2 ) |> Enum.reduce( %{}, fn( { k, v }, acc ) -> Map.put( acc, String.to_atom( k ), v ) end )
	def zip( list1, list2, :no_atom ), do: Enum.zip( list1, list2 ) |> Enum.into( %{} )
	def zip( list1, list2 ),           do: zip( list1, list2, :no_atom )

	@doc """
	Zip columns list and list of rows list

	## Examples
		iex> Lst.columns_rows( [ "c1", "c2", "c3" ], [ [ "v1", 2, true ], [ "v2", 5, false ] ] )
		[ %{ "c1" => "v1", "c2" => 2, "c3" => true }, %{ "c1" => "v2", "c2" => 5, "c3" => false } ]
		iex> Lst.columns_rows( [ "c1", "c2", "c3" ], [ [ "v1", 2, true ], [ "v2", 5, false ] ], :atom )
		[ %{ c1: "v1", c2: 2, c3: true }, %{ c1: "v2", c2: 5, c3: false } ]
	"""
	def columns_rows( columns, rows, :atom ),    do: rows |> Enum.map( & zip( columns, &1, :atom ) )
	def columns_rows( columns, rows, :no_atom ), do: rows |> Enum.map( & zip( columns, &1 ) )
	def columns_rows( columns, rows ),           do: columns_rows( columns, rows, :no_atom )

	@doc """
	Separate

	## Examples
		iex> Lst.separate( [ [ "c1", "c2", "c3" ], [ "v1", 2, true ], [ "v2", 5, false ] ], "columns", "rows" )
		%{ "columns" => [ "c1", "c2", "c3" ], "rows" => [ [ "v1", 2, true ], [ "v2", 5, false ] ] }
	"""
	def separate( list, head_name, tail_name ) do
		[ head | tail ] = list
		%{ head_name => head, tail_name => tail }
	end

	@doc """
	Delete keyword list items by keys(string)

	## Examples
		iex> Lst.delete_by_keys( [ c1: 6, c2: 7, c3: 8, c4: 9, c5: 0 ], [ "c2", "c4" ] )
		[ c1: 6, c3: 8, c5: 0 ]
		iex> Lst.delete_by_keys( [ c1: 6, c2: 7, c3: 8, c4: 9, c5: 0 ], [ "c3", "c1" ] )
		[ c2: 7, c4: 9, c5: 0 ]
	"""
	def delete_by_keys( list, names ) do
		names |> Enum.reduce( list, fn name, acc -> Keyword.delete( acc, String.to_atom( name ) ) end )
	end

	@doc """
	String list to atom list

	## Examples
		iex> Lst.to_atoms_from_strings( [ "c1", "c2", "c3" ] )
		[ :c1, :c2, :c3 ]
	"""
	def to_atoms_from_strings( list ) do
		list |> Enum.map( & String.to_atom( &1 ) )
	end

	@doc """
	Pickup match lists

	## Examples
		iex> Lst.pickup_match( [ "c1", "c2", "c3", "c4", "c5" ], [ "c2", "c4" ] )
		[ "c2", "c4" ]
	"""
	def pickup_match( list1, list2 ) do
		list1 |> Enum.filter( fn item1 -> Enum.find( list2, fn item2 -> item1 == item2 end ) != nil end )
	end

	@doc """
	Pickup match index lists

	## Examples
		iex> Lst.pickup_match_index( [ "c1", "c2", "c3", "c4", "c5", "c6" ], [ "c1", "c3", "c6" ] )
		[ 5, 2, 0 ]
	"""
	def pickup_match_index( columns, find_column_names ) do
		find_column_names |> Enum.reduce( [], fn find_column_name, acc ->
			acc ++ [ Enum.find_index( columns, & &1 == find_column_name ) ]
		end )
		|> Enum.reverse
	end

	@doc """
	Pickup unmatch lists

	## Examples
		iex> Lst.pickup_unmatch( [ "c1", "c2", "c3", "c4", "c5" ], [ "c2", "c4" ] )
		[ "c1", "c3", "c5" ]
	"""
	def pickup_unmatch( list1, list2 ) do
		list1 |> Enum.filter( fn item1 -> Enum.find( list2, fn item2 -> item1 == item2 end ) == nil end )
	end

	@doc """
	Pickup match list and in map lists

	## Examples
		iex> Lst.pickup_match_from_map( [ %{ "name" => "c2", "age" => 21 }, %{ "name" => "c4", "age" => 42 } ], [ "c1", "c2", "c3", "c4", "c5" ], "name" )
		[ %{ "name" => "c2", "age" => 21 }, %{ "name" => "c4", "age" => 42 } ]
		iex> Lst.pickup_match_from_map( [ %{ "name" => "c2", "age" => 21 }, %{ "name" => "c6", "age" => 84 } ], [ "c1", "c2", "c3", "c4", "c5" ], "name" )
		[ %{ "name" => "c2", "age" => 21 } ]
	"""
	def pickup_match_from_map( map_list, list, map_list_key ) do
		map_list |> Enum.map( fn map -> if Enum.find( list, & &1 == map[ map_list_key ] ) != nil, do: map, else: nil end )
		|> List.delete( nil )
	end

	@doc """
	List from map

	## Examples
		iex> Lst.list_from_map( [ %{ "name" => "c2", "age" => 21 }, %{ "name" => "c4", "age" => 42 } ], "name" )
		[ "c2", "c4" ]
		iex> Lst.list_from_map( [ %{ "name" => "c2", "items" => [ 11, 21, 31 ] }, %{ "name" => "c4", "items" => [ 21, 22, 32 ] } ], "items", 1 )
		[ 21, 22 ]
	"""
	def list_from_map( map_list, key ) do
		map_list |> Enum.map( & &1[ key ] )
	end
	def list_from_map( map_list, key, index ) do
		map_list |> Enum.map( & Enum.at( &1[ key ], index ) )
	end

	@doc """
	Merge lists

	## Examples
		iex> Lst.merge( [ [ "c4", "c5", "c6" ], [ "c7", "c8", "c9" ] ], [ "c1", "c2" ] )
		[ [ "c1", "c4", "c5", "c6" ], [ "c2", "c7", "c8", "c9" ] ]
	"""
	def merge( list_list, add_list ) do
		Enum.zip( list_list, add_list ) |> Enum.map( fn { list, value } -> [ value ] ++ list end )
	end
end

defmodule Type do
	@moduledoc """
	Type library.
	"""

	@doc """
	Type check

	## Examples
		iex> Type.is( nil )
		"nil"
		iex> Type.is( "v1" )
		"String"
		iex> Type.is( "2" )
		"Integer"
		iex> Type.is( 2 )
		"Integer"
		iex> Type.is( "true" )
		"Boolean"
		iex> Type.is( true )
		"Boolean"
		iex> Type.is( "false" )
		"Boolean"
		iex> Type.is( false )
		"Boolean"
		iex> Type.is( "12.34" )
		"Float"
		iex> Type.is( 12.34 )
		"Float"
		iex> Type.is( %{ "cs" => "v1", "ci" => "2", "cb" => "true", "cf" => "12.34" } )
		%{ "cs" => "String", "ci" => "Integer", "cb" => "Boolean", "cf" => "Float" }
		iex> Type.is( %{ "cs" => "v1", "ci" => 2, "cb" => true, "cf" => 12.34 } )
		%{ "cs" => "String", "ci" => "Integer", "cb" => "Boolean", "cf" => "Float" }
		iex> Type.is( [ "v1", 2, true, 12.34 ] )
		[ "String", "Integer", "Boolean", "Float" ]
	"""
	def is( map )  when is_map( map ),   do: map  |> Enum.reduce( %{}, fn { k, v }, acc -> Map.put( acc, k, is( v ) ) end )
	def is( list ) when is_list( list ), do: list |> Enum.map( &is( &1 ) )
	def is( nil ), do: "nil"
	def is( value ) when is_binary( value ) do
		cond do
			is_boolean_include_string( value ) -> "Boolean"
			is_float_include_string( value   ) -> "Float"
			is_integer_include_string( value ) -> "Integer"
			true                               -> "String"
		end
	end
	def is( value ) when is_boolean( value ), do: "Boolean"
	def is( value ) when is_float( value ),   do: "Float"
	def is( value ) when is_integer( value ), do: "Integer"

	def is_boolean_include_string( value ) when is_binary( value ) do
		String.downcase( value ) == "true" || String.downcase( value ) == "false"
	end
	def is_boolean_include_string( value ), do: is_boolean( value )

	def is_float_include_string( value ) when is_binary( value ) do
		try do
			if String.to_float( value ), do: true, else: false
		catch
			_, _ -> false
		end
	end
	def is_float_include_string( value ), do: is_float( value )

	def is_integer_include_string( value ) when is_binary( value ) do
		try do
			if String.to_integer( value ), do: true, else: false
		catch
			_, _ -> false
		end
	end
	def is_integer_include_string( value ), do: is_integer( value )

	def is_datetime( value ) do
		try do
			if Dt.to_datetime( value ) |> is_map, do: true, else: false
		catch
			_, _ -> false
		end
	end

	@doc """
	aa

	## Examples
		iex> Type.is_empty( nil )
		true
		iex> Type.is_empty( "" )
		true
		iex> Type.is_empty( "abc" )
		false
		iex> Type.is_empty( 123 )
		false
		iex> Type.is_empty( 12.34 )
		false
	"""
	def is_empty( nil ), do: true
	def is_empty( "" ),  do: true
	def is_empty( _ ),   do: false

	@doc """
	aa

	## Examples
		iex> Type.float( nil )
		"NaN"
		iex> Type.float( "" )
		"NaN"
		iex> Type.float( "12.34567" )
		"12.35"
		iex> Type.float( "12.34444" )
		"12.34"
		iex> Type.float( 12.34567 )
		"12.35"
		iex> Type.float( 12.34444 )
		"12.34"
	"""
	def float( nil ), do: "NaN"
	def float( "" ),  do: "NaN"
	def float( value ) when is_number( value ) do
		case is( value ) do
			"Float"   -> value |> Number.to_string( 2 )
			"Integer" -> value |> Number.to_string
		end
	end
	def float( value ) when is_binary( value ) do
		case is( value ) do
			"Float"   -> value |> String.to_float   |> Number.to_string( 2 )
			"Integer" -> value |> String.to_integer |> Number.to_string
		end
	end

	@doc """
	aa

	## Examples
		iex> Type.to_number( nil )
		nil
		iex> Type.to_number( "123" )
		123
		iex> Type.to_number( "12.34" )
		12.34
		iex> Type.to_number( 123 )
		123
		iex> Type.to_number( 12.34 )
		12.34
		iex> Type.to_number( "" )
		nil
		iex> Type.to_number( "abc" )
		nil
	"""
	def to_number( nil ),  do: nil
	def to_number( value ) when is_number( value ), do: value
	def to_number( value ) when is_binary( value ) do
		case is( value ) do
			"Float"   -> value |> String.to_float
			"Integer" -> value |> String.to_integer
			_         -> nil
		end
	end

	@doc """
	To string

	## Examples
		iex> Type.to_string( nil )
		""

		iex> Type.to_string( 123 )
		"123"

		iex> Type.to_string( 12.34 )
		"12.34"

		iex> Type.to_string( "123" )
		"123"

		iex> Type.to_string( "12.34" )
		"12.34"

		iex> Type.to_string( ~N[2015-01-28 01:15:52.000000] )
		"2015-01-28T01:15:52.000Z"
	"""
	def to_string( nil ),  do: ""
	def to_string( value ) when is_binary( value ), do: value
	def to_string( value ) when is_number( value ) do
		case is( value ) do
			"Float"   -> value |> Float.to_string
			"Integer" -> value |> Integer.to_string
			_         -> nil
		end
	end
	def to_string( value ) when is_map( value ) do
		if is_datetime( value ) do
			Dt.to_datetime( value ) |> Dt.to_string( "%Y-%0m-%0dT%0H:%0M:%0S.%0LZ" )
		else
			inspect( value )
		end
	end






	def to_string_datetime( value ) when is_map( value ), do: Dt.to_string( value, "%Y-%0m-%0dT%0H:%0M:%0S.%0LZ" )
	def to_string_datetime( value ), do: value

	@doc """
	Possible types(not collentions)

	## Examples
		iex> Type.possible_types( "" )
		[ nil, nil, "String", nil ]
		iex> Type.possible_types( "1" )
		[ "Integer", nil, "String", nil ]
		iex> Type.possible_types( 1 )
		[ "Integer", nil, nil, nil ]
		iex> Type.possible_types( "1.2" )
		[ nil, "Float", "String", nil ]
		iex> Type.possible_types( 1.2 )
		[ nil, "Float", nil, nil ]
		iex> Type.possible_types( true )
		[ nil, nil, nil, "Boolean" ]
		iex> Type.possible_types( "true" )
		[ nil, nil, "String", "Boolean" ]
		iex> Type.possible_types( "True" )
		[ nil, nil, "String", "Boolean" ]
		iex> Type.possible_types( false )
		[ nil, nil, nil, "Boolean" ]
		iex> Type.possible_types( "false" )
		[ nil, nil, "String", "Boolean" ]
		iex> Type.possible_types( "False" )
		[ nil, nil, "String", "Boolean" ]
	"""
	def possible_types( value ) do
		[
			%{ "label" => "Integer", "checker" => &is_integer_include_string/1 },
			%{ "label" => "Float",   "checker" => &is_float_include_string/1 },
			%{ "label" => "String",  "checker" => &is_binary/1 },
			%{ "label" => "Boolean", "checker" => &is_boolean_include_string/1 },
		]
		|> Enum.map( &( if &1[ "checker" ].( value ) == true, do: &1[ "label" ], else: nil ) )
	end

	@doc """
	Is missing

	## Examples
		iex> Type.is_missing( "" )
		true
		iex> Type.is_missing( nil )
		true
		iex> Type.is_missing( "a" )
		false
		iex> Type.is_missing( 1 )
		false
		iex> Type.is_missing( 1.2 )
		false
		iex> Type.is_missing( true )
		false
	"""
	def is_missing( value ), do: value == nil || value == ""

end
