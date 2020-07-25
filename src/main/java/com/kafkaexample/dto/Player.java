package com.kafkaexample.dto;

public class Player
{
	private int jerseyNumber;
	private String sportName;
	private int age;
	private double height;
	private String name;

	public Player()
	{
	}

	public Player(String name, int jerseyNumber, String sportName, int age, double height)
	{
		this.name = name;
		this.jerseyNumber = jerseyNumber;
		this.sportName = sportName;
		this.age = age;
		this.height = height;
	}

	@Override
	public String toString()
	{
		return "Player [jerseyNumber=" + jerseyNumber + ", sportName=" + sportName + ", age=" + age + ", height=" + height + ", name=" + name + "]";
	}

	public double getHeight()
	{
		return height;
	}

	public void setHeight(double height)
	{
		this.height = height;
	}

	public int getAge()
	{
		return age;
	}

	public void setAge(int age)
	{
		this.age = age;
	}

	public String getSportName()
	{
		return sportName;
	}

	public void setSportName(String sportName)
	{
		this.sportName = sportName;
	}

	public int getJerseyNumber()
	{
		return jerseyNumber;
	}

	public void setJerseyNumber(int jerseyNumber)
	{
		this.jerseyNumber = jerseyNumber;
	}
	
	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}
}
