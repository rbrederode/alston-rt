import inspect
import witmotion

def recusive_module_search(module):
    members = inspect.getmembers(module)

    for name, member in members:
        try:
            if inspect.ismodule(member):
                # Dont go too deep :)
                if member is module:
                    recusive_module_search(member)
            elif inspect.isfunction(member):
                file = inspect.getfile(member)
                print(file, function_signature_string(member), "function")
            elif inspect.isclass(member):
                file = inspect.getfile(member)
                print(file, function_signature_string(member), "class")
                class_members = inspect.getmembers(member)
                for name, class_member in class_members:
                    if inspect.isfunction(class_member):
                        member_args = inspect.signature(class_member)
                        print(file, member.__name__ + "." + function_signature_string(class_member), "method")
        except Exception as e:
            print(f"Error inspecting {member}: {e}")

def function_signature_string(member):
    parameters = inspect.signature(member).parameters
    return member.__name__ + "(" + ', '.join(str(x) for x in parameters.values()) + ")"

def main():
    recusive_module_search(witmotion.protocol.AccelerationMessage)

if __name__ == "__main__":
    main()