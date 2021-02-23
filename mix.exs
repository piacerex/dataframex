defmodule Dataframex.Mixfile do
  use Mix.Project

  def project do
    [
      app: :dataframex,
      version: "0.5.0",
      elixir: "~> 1.11",
		description: "Elixir dataframe utilities", 
		package: 
		[
			maintainers: [ "piacerex", "the_haigo" ], 
			licenses:    [ "Apache 2.0" ], 
			links:       %{ "GitHub" => "https://github.com/piacerex/dataframex" }, 
		],
      start_permanent: Mix.env == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
	defp deps do
		[
			{ :ex_doc,         "~> 0.19",   only: :dev, runtime: false }, 
			{ :earmark,        "~> 1.2",    only: :dev }, 
			{ :mix_test_watch, "~> 0.6",    only: :dev, runtime: false }, 
			{ :dialyxir,       "~> 0.5.1",  only: :dev }, 
		]
	end
end
