defmodule Plausible.Time do
  @moduledoc """
  Time API.
  """

  @spec diff(
          Date.t() | DateTime.t() | NaiveDateTime.t(),
          :month | :week | :day | :hour | :minute | System.time_unit()
        ) :: integer()
  def diff(%Date{year: y1, month: m1, day: d1}, %Date{year: y2, month: m2, day: d2}, :month) do
    months = (y1 - y2) * 12 + m1 - m2
    days_in_month2 = Calendar.days_in_month(y2, m2)

    cond do
      months < 0 && d2 < d1 && (days_in_month2 >= d1 || days_in_month2 != d2) ->
        months + 1

      months > 0 && d2 > d1 ->
        months - 1

      true ->
        months
    end
  end

  def diff(%Date{} = d1, %Date{} = d2, :week) do
    days = diff(d1, d2, :day)

    div(days, 7)
  end

  def diff(%Date{} = d1, %Date{} = d2, :day) do
    Date.diff(d1, d2)
  end

  def diff(%Date{} = d1, %Date{} = d2, interval) do
    NaiveDateTime.diff(
      NaiveDateTime.new(d1, ~T[00:00:00]),
      NaiveDateTime.new(d2, ~T[00:00:00]),
      interval
    )
  end

  def diff(%DateTime{} = dt1, %DateTime{} = dt2, interval) when interval in [:week, :month] do
    diff(DateTime.to_date(dt1), DateTime.to_date(dt2), interval)
  end

  def diff(%DateTime{} = dt1, %DateTime{} = dt2, interval) do
    DateTime.diff(dt1, dt2, interval)
  end

  def diff(%NaiveDateTime{} = nd1, %NaiveDateTime{} = nd2, interval)
      when interval in [:week, :month] do
    diff(NaiveDateTime.to_date(dt1), NaiveDateTime.to_date(dt2), interval)
  end

  def diff(%NaiveDateTime{} = nd1, %NaiveDateTime{} = nd2, interval) do
    NaiveDateTime.diff(nd1, nd2, interval)
  end

  def beginning_of_year(%DateTime{} = dt) do
    truncated = DateTime.truncate(dt, :second)

    %{truncated | minute: 0, hour: 0, day: 1, month: 1}
  end

  def beginning_of_year(%Date{} = date) do
    Date.new!(date.year, 1, 1)
  end

  def end_of_year(%Date{} = date) do
    date
    |> beginning_of_year()
    |> Date.shift(year: 1, day: -1)
  end
end
