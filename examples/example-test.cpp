
#include <iostream>
#include "make_factory.hpp"
#include "make_cluster.hpp"

void print_status(bool status, std::string name)
{
	std::cout<<(status ? "[OK] " : "[Failed] ")<<name<<std::endl;
}

int main()
{
	std::cout<<"=============================\n";
	std::cout<<"     Testing Examples\n";
	std::cout<<"-----------------------------"<<std::endl;

	auto status = make_factory::run();
	print_status(status, "make_factory");

	status = make_cluster::run();
	print_status(status, "make_cluster");
}