package com.dto;

public class Player
{
	private String sportName;

	private int age;

	private double height;

	public Player()
	{
	}

	public Player(String sportName, int age, double height)
	{
		this.sportName = sportName;
		this.age = age;
		this.height = height;
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

	@Override
	public String toString()
	{
		return "Player [sportName=" + sportName + ", age=" + age + ", height=" + height + "]";
	}
}
